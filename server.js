const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, perMessageDeflate: false });

const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// --- IN-MEMORY STORAGE ---
// MVP: esses dados ficam em memória. Próximo passo de produção: PostgreSQL.
let chargers = [];
let events = [];
let sessions = [];
let meterValues = [];
let commandResults = [];

let tariffs = [
  { id: crypto.randomUUID(), name: 'Madrugada', startHour: 0, endHour: 6, pricePerKwh: 1.5, active: true },
  { id: crypto.randomUUID(), name: 'Horário Comercial', startHour: 6, endHour: 18, pricePerKwh: 2.2, active: true },
  { id: crypto.randomUUID(), name: 'Horário de Ponta', startHour: 18, endHour: 22, pricePerKwh: 2.8, active: true },
  { id: crypto.randomUUID(), name: 'Noite', startHour: 22, endHour: 24, pricePerKwh: 1.9, active: true }
];

const activeConnections = new Map(); // charge_point_id -> WebSocket
const pendingCommands = new Map(); // messageId -> command metadata

// --- HELPERS ---
function nowIso() {
  return new Date().toISOString();
}

function limitArray(arr, max = 500) {
  while (arr.length > max) arr.pop();
}

function addEvent({ chargePointId, direction, action, payload }) {
  const event = {
    id: crypto.randomUUID(),
    charge_point_id: chargePointId,
    direction,
    action,
    payload: typeof payload === 'string' ? payload : JSON.stringify(payload || {}),
    created_at: nowIso()
  };

  events.unshift(event);
  limitArray(events, 500);
  return event;
}

function getCurrentTariff(date = new Date()) {
  const hour = date.getHours();

  const tariff = tariffs.find(t => {
    if (!t.active) return false;
    if (t.startHour < t.endHour) return hour >= t.startHour && hour < t.endHour;
    return hour >= t.startHour || hour < t.endHour;
  });

  return tariff || tariffs.find(t => t.active) || tariffs[0] || null;
}

function calculateCost(energyKwh, date = new Date()) {
  const tariff = getCurrentTariff(date);
  const pricePerKwh = tariff ? Number(tariff.pricePerKwh) : 0;
  const currentCost = Number((Number(energyKwh || 0) * pricePerKwh).toFixed(2));

  return {
    tariff,
    price_per_kwh: pricePerKwh,
    current_cost: currentCost
  };
}

function getOrCreateConnector(charger, connectorId) {
  const number = Number(connectorId || 1);
  if (!charger.connectors) charger.connectors = [];

  let connector = charger.connectors.find(c => c.connector_number === number);
  if (!connector) {
    connector = {
      id: crypto.randomUUID(),
      connector_number: number,
      status: 'Unknown',
      error_code: 'NoError',
      info: null,
      vendor_error_code: null,
      timestamp: null
    };
    charger.connectors.push(connector);
  }

  return connector;
}

function normalizeEnergyToKwh(value, unit) {
  const numeric = Number.parseFloat(value);
  if (Number.isNaN(numeric)) return null;
  if (unit === 'Wh') return numeric / 1000;
  if (unit === 'kWh') return numeric;
  if (!unit && numeric > 10000) return numeric / 1000;
  return numeric;
}

function normalizePowerToKw(value, unit) {
  const numeric = Number.parseFloat(value);
  if (Number.isNaN(numeric)) return null;
  if (unit === 'W') return numeric / 1000;
  if (unit === 'kW') return numeric;
  return numeric;
}

function findSampledValue(sampledValues, measurands) {
  if (!Array.isArray(sampledValues)) return null;
  return sampledValues.find(sv => measurands.includes(sv.measurand)) || null;
}

function sendCallResult(ws, chargePointId, messageId, action, payload) {
  console.log(`[OCPP] [${chargePointId}] Enviando CALLRESULT para ${action}:`, JSON.stringify(payload));
  ws.send(JSON.stringify([3, messageId, payload]));
}

function sendCallError(ws, chargePointId, messageId, errorCode, errorDescription, details = {}) {
  console.error(`[OCPP] [${chargePointId}] Enviando CALLERROR: ${errorCode} - ${errorDescription}`);
  ws.send(JSON.stringify([4, messageId, errorCode, errorDescription, details]));
}

function findCharger(chargerId) {
  return chargers.find(c => c.id === chargerId || c.charge_point_id === chargerId);
}

// --- WEBSOCKET OCPP SERVER ---
wss.on('connection', (ws, req) => {
  const urlMatch = req.url.match(/^\/ocpp\/(.+)$/);

  if (!urlMatch) {
    console.warn(`[OCPP] Conexão rejeitada: Rota inválida (${req.url}). Esperado /ocpp/:chargePointId`);
    ws.close(1008, 'Invalid route');
    return;
  }

  const chargePointId = decodeURIComponent(urlMatch[1]);
  console.log(`[OCPP] [${chargePointId}] Carregador conectado na rota ${req.url}`);

  activeConnections.set(chargePointId, ws);

  let charger = chargers.find(c => c.charge_point_id === chargePointId);
  if (charger) {
    charger.status = 'Online';
    charger.ultimo_heartbeat = nowIso();
  } else {
    charger = {
      id: crypto.randomUUID(),
      charge_point_id: chargePointId,
      fabricante: 'Desconhecido',
      modelo: 'Desconhecido',
      status: 'Online',
      ultimo_heartbeat: nowIso(),
      connectors: [{
        id: crypto.randomUUID(),
        connector_number: 1,
        status: 'Available',
        error_code: 'NoError',
        info: null,
        vendor_error_code: null,
        timestamp: null
      }]
    };
    chargers.push(charger);
  }

  ws.on('message', (message) => {
    let parsed;

    try {
      parsed = JSON.parse(message);
    } catch (err) {
      console.error(`[OCPP] [${chargePointId}] Erro de Parse JSON. Mensagem recebida:`, message.toString());
      return;
    }

    try {
      if (Array.isArray(parsed) && parsed[0] === 2) {
        const [, messageId, action, payload = {}] = parsed;
        console.log(`[OCPP] [${chargePointId}] Recebeu CALL: ${action}`, JSON.stringify(payload));

        addEvent({ chargePointId, direction: 'IN', action, payload });

        let responsePayload = {};

        if (action === 'BootNotification') {
          responsePayload = { status: 'Accepted', currentTime: nowIso(), interval: 300 };
          if (payload.chargePointVendor) charger.fabricante = payload.chargePointVendor;
          if (payload.chargePointModel) charger.modelo = payload.chargePointModel;
          charger.status = 'Online';
          charger.ultimo_heartbeat = nowIso();

        } else if (action === 'Heartbeat') {
          responsePayload = { currentTime: nowIso() };
          charger.status = 'Online';
          charger.ultimo_heartbeat = nowIso();

        } else if (action === 'Authorize') {
          console.log(`[OCPP] [${chargePointId}] Processando Authorize (idTag: ${payload.idTag})`);
          // MVP: aceita qualquer idTag. Depois validar usuário/RFID/saldo.
          responsePayload = { idTagInfo: { status: 'Accepted' } };

        } else if (action === 'StatusNotification') {
          console.log(`[OCPP] [${chargePointId}] StatusNotification conector=${payload.connectorId} status=${payload.status} erro=${payload.errorCode}`);
          responsePayload = {};

          const connector = getOrCreateConnector(charger, payload.connectorId || 1);
          connector.status = payload.status || connector.status;
          connector.error_code = payload.errorCode || 'NoError';
          connector.info = payload.info || null;
          connector.vendor_error_code = payload.vendorErrorCode || null;
          connector.timestamp = payload.timestamp || nowIso();

          charger.status = 'Online';
          charger.ultimo_heartbeat = nowIso();

        } else if (action === 'StartTransaction') {
          const transactionId = Math.floor(Math.random() * 1000000);
          const meterStartKwh = normalizeEnergyToKwh(payload.meterStart, payload.unit) || 0;
          const costInfo = calculateCost(0);

          responsePayload = { transactionId, idTagInfo: { status: 'Accepted' } };

          sessions.unshift({
            id: crypto.randomUUID(),
            charger_id: charger.id,
            charge_point_id: chargePointId,
            connector_id: payload.connectorId || 1,
            transaction_id: transactionId,
            id_tag: payload.idTag || null,
            started_at: payload.timestamp || nowIso(),
            ended_at: null,
            meter_start_kwh: meterStartKwh,
            meter_stop_kwh: null,
            energy_kwh: 0,
            current_power_kw: 0,
            price_per_kwh: costInfo.price_per_kwh,
            current_cost: 0,
            tariff_name: costInfo.tariff ? costInfo.tariff.name : null,
            status: 'Charging'
          });
          limitArray(sessions, 300);

          const connector = getOrCreateConnector(charger, payload.connectorId || 1);
          connector.status = 'Occupied';

        } else if (action === 'MeterValues') {
          responsePayload = {};
          const session = sessions.find(s => s.transaction_id === payload.transactionId && s.status === 'Charging');

          if (payload.meterValue && payload.meterValue.length > 0) {
            const meterValue = payload.meterValue[payload.meterValue.length - 1];
            const sampledValues = meterValue.sampledValue || [];

            const energyValue = findSampledValue(sampledValues, ['Energy.Active.Import.Register']) || sampledValues[0];
            const powerValue = findSampledValue(sampledValues, ['Power.Active.Import', 'Power.Active.Import.Register']);
            const voltageValue = findSampledValue(sampledValues, ['Voltage']);
            const currentValue = findSampledValue(sampledValues, ['Current.Import', 'Current.Offered']);

            const energyKwh = energyValue ? normalizeEnergyToKwh(energyValue.value, energyValue.unit) : null;
            const powerKw = powerValue ? normalizePowerToKw(powerValue.value, powerValue.unit) : null;
            const voltage = voltageValue ? Number.parseFloat(voltageValue.value) : null;
            const current = currentValue ? Number.parseFloat(currentValue.value) : null;

            const record = {
              id: crypto.randomUUID(),
              charge_point_id: chargePointId,
              transaction_id: payload.transactionId,
              connector_id: payload.connectorId || null,
              timestamp: meterValue.timestamp || nowIso(),
              energy_kwh: energyKwh,
              power_kw: powerKw,
              voltage,
              current,
              raw_payload: payload,
              created_at: nowIso()
            };

            meterValues.unshift(record);
            limitArray(meterValues, 1000);

            if (session) {
              if (energyKwh !== null) {
                const consumed = Math.max(0, energyKwh - (session.meter_start_kwh || 0));
                session.energy_kwh = Number(consumed.toFixed(3));
              }
              if (powerKw !== null) session.current_power_kw = Number(powerKw.toFixed(3));

              const costInfo = calculateCost(session.energy_kwh);
              session.price_per_kwh = costInfo.price_per_kwh;
              session.current_cost = costInfo.current_cost;
              session.tariff_name = costInfo.tariff ? costInfo.tariff.name : null;
            }
          }

        } else if (action === 'StopTransaction') {
          responsePayload = { idTagInfo: { status: 'Accepted' } };

          const session = sessions.find(s => s.transaction_id === payload.transactionId);
          if (session) {
            session.ended_at = payload.timestamp || nowIso();
            session.status = 'Completed';

            if (payload.meterStop !== undefined) {
              const finalMeterKwh = normalizeEnergyToKwh(payload.meterStop, payload.unit);
              if (finalMeterKwh !== null) {
                session.meter_stop_kwh = finalMeterKwh;
                session.energy_kwh = Number(Math.max(0, finalMeterKwh - (session.meter_start_kwh || 0)).toFixed(3));
              }
            }

            const costInfo = calculateCost(session.energy_kwh);
            session.price_per_kwh = costInfo.price_per_kwh;
            session.current_cost = costInfo.current_cost;
            session.tariff_name = costInfo.tariff ? costInfo.tariff.name : null;
          }

          const connectorId = session ? session.connector_id : 1;
          const connector = getOrCreateConnector(charger, connectorId);
          connector.status = 'Available';

        } else {
          console.log(`[OCPP] [${chargePointId}] Ação não tratada especificamente: ${action}. Respondendo vazio para manter compatibilidade.`);
          responsePayload = {};
        }

        sendCallResult(ws, chargePointId, messageId, action, responsePayload);

      } else if (Array.isArray(parsed) && parsed[0] === 3) {
        const [, messageId, payload] = parsed;
        const pending = pendingCommands.get(messageId);

        console.log(`[OCPP] [${chargePointId}] Recebeu CALLRESULT (Id: ${messageId}):`, JSON.stringify(payload));

        commandResults.unshift({
          id: crypto.randomUUID(),
          message_id: messageId,
          charge_point_id: chargePointId,
          action: pending ? pending.action : 'Unknown',
          status: 'Accepted',
          request_payload: pending ? pending.payload : null,
          response_payload: payload,
          created_at: nowIso()
        });
        limitArray(commandResults, 300);

        addEvent({
          chargePointId,
          direction: 'IN',
          action: pending ? `${pending.action}Result` : 'CALLRESULT',
          payload
        });

        pendingCommands.delete(messageId);

      } else if (Array.isArray(parsed) && parsed[0] === 4) {
        const [, messageId, errorCode, errorDescription, errorDetails] = parsed;
        const pending = pendingCommands.get(messageId);

        console.error(`[OCPP] [${chargePointId}] Recebeu CALLERROR (Id: ${messageId}):`, errorCode, errorDescription);

        commandResults.unshift({
          id: crypto.randomUUID(),
          message_id: messageId,
          charge_point_id: chargePointId,
          action: pending ? pending.action : 'Unknown',
          status: 'Error',
          request_payload: pending ? pending.payload : null,
          error_code: errorCode,
          error_description: errorDescription,
          error_details: errorDetails || {},
          created_at: nowIso()
        });
        limitArray(commandResults, 300);

        addEvent({
          chargePointId,
          direction: 'IN',
          action: pending ? `${pending.action}Error` : 'CALLERROR',
          payload: { errorCode, errorDescription, errorDetails }
        });

        pendingCommands.delete(messageId);

      } else {
        console.warn(`[OCPP] [${chargePointId}] Formato de mensagem OCPP desconhecido:`, parsed);
      }
    } catch (err) {
      console.error(`[OCPP] [${chargePointId}] Erro interno ao processar mensagem:`, err);
      if (Array.isArray(parsed) && parsed[0] === 2) {
        sendCallError(ws, chargePointId, parsed[1], 'InternalError', err.message || 'Internal server error');
      }
    }
  });

  ws.on('close', (code, reason) => {
    console.log(`[OCPP] [${chargePointId}] Carregador desconectado (Código: ${code}, Motivo: ${reason || 'N/A'})`);
    activeConnections.delete(chargePointId);
    if (charger) charger.status = 'Offline';
  });

  ws.on('error', (error) => {
    console.error(`[OCPP] [${chargePointId}] Erro na conexão WebSocket:`, error);
  });
});

// --- REST API ROUTES ---
app.get('/', (req, res) => {
  res.json({
    name: 'Fotus Charge OCPP Backend',
    status: 'online',
    websocket: '/ocpp/{chargePointId}',
    timestamp: nowIso()
  });
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: nowIso() });
});

app.get('/api/dashboard-stats', (req, res) => {
  const total = chargers.length;
  const online = chargers.filter(c => c.status === 'Online').length;
  const offline = total - online;

  let availableConnectors = 0;
  chargers.forEach(c => {
    if (c.connectors) {
      availableConnectors += c.connectors.filter(conn => conn.status === 'Available').length;
    }
  });

  const activeSessions = sessions.filter(s => s.status === 'Charging').length;
  const totalEnergy = sessions.reduce((acc, s) => acc + (s.energy_kwh || 0), 0);
  const totalRevenue = sessions.reduce((acc, s) => acc + (s.current_cost || 0), 0);

  res.json({
    chargers: { total, online, offline },
    connectors: { available: availableConnectors },
    sessions: {
      active: activeSessions,
      totalEnergy: Number(totalEnergy.toFixed(3)),
      totalRevenue: Number(totalRevenue.toFixed(2))
    },
    tariff: getCurrentTariff()
  });
});

app.get('/api/events', (req, res) => {
  res.json(events.slice(0, Number(req.query.limit || 50)));
});

app.get('/api/chargers', (req, res) => {
  res.json(chargers);
});

app.get('/api/chargers/:id', (req, res) => {
  const charger = findCharger(req.params.id);
  if (charger) return res.json(charger);
  res.status(404).json({ error: 'Carregador não encontrado' });
});

app.get('/api/logs/:id', (req, res) => {
  const charger = findCharger(req.params.id);
  if (!charger) return res.status(404).json({ error: 'Carregador não encontrado' });
  const chargerEvents = events.filter(e => e.charge_point_id === charger.charge_point_id);
  res.json(chargerEvents.slice(0, Number(req.query.limit || 50)));
});

app.get('/api/sessions', (req, res) => {
  res.json(sessions);
});

app.get('/api/meter-values', (req, res) => {
  const { transactionId, chargePointId } = req.query;
  let data = meterValues;

  if (transactionId) data = data.filter(m => String(m.transaction_id) === String(transactionId));
  if (chargePointId) data = data.filter(m => m.charge_point_id === chargePointId);

  res.json(data.slice(0, Number(req.query.limit || 100)));
});

app.get('/api/command-results', (req, res) => {
  res.json(commandResults.slice(0, Number(req.query.limit || 100)));
});

// --- TARIFF ROUTES ---
app.get('/api/tariffs', (req, res) => {
  res.json(tariffs);
});

app.get('/api/tariffs/current', (req, res) => {
  res.json(getCurrentTariff());
});

app.post('/api/tariffs', (req, res) => {
  const { name, startHour, endHour, pricePerKwh, active } = req.body;

  if (!name || startHour === undefined || endHour === undefined || pricePerKwh === undefined) {
    return res.status(400).json({ error: 'Campos obrigatórios: name, startHour, endHour, pricePerKwh' });
  }

  const tariff = {
    id: crypto.randomUUID(),
    name,
    startHour: Number(startHour),
    endHour: Number(endHour),
    pricePerKwh: Number(pricePerKwh),
    active: active !== false
  };

  tariffs.push(tariff);
  res.status(201).json(tariff);
});

app.put('/api/tariffs/:id', (req, res) => {
  const tariff = tariffs.find(t => t.id === req.params.id);
  if (!tariff) return res.status(404).json({ error: 'Tarifa não encontrada' });

  const { name, startHour, endHour, pricePerKwh, active } = req.body;
  if (name !== undefined) tariff.name = name;
  if (startHour !== undefined) tariff.startHour = Number(startHour);
  if (endHour !== undefined) tariff.endHour = Number(endHour);
  if (pricePerKwh !== undefined) tariff.pricePerKwh = Number(pricePerKwh);
  if (active !== undefined) tariff.active = Boolean(active);

  res.json(tariff);
});

app.delete('/api/tariffs/:id', (req, res) => {
  const before = tariffs.length;
  tariffs = tariffs.filter(t => t.id !== req.params.id);
  if (tariffs.length === before) return res.status(404).json({ error: 'Tarifa não encontrada' });
  res.json({ success: true });
});

// --- COMMAND ROUTES (POST) ---
function sendCommand(chargerId, action, payload, res) {
  const charger = findCharger(chargerId);
  if (!charger) return res.status(404).json({ error: 'Carregador não encontrado' });

  const ws = activeConnections.get(charger.charge_point_id);
  if (!ws || ws.readyState !== 1) return res.status(400).json({ error: 'Carregador está offline' });

  const messageId = crypto.randomUUID();
  const message = [2, messageId, action, payload || {}];

  console.log(`[OCPP] [${charger.charge_point_id}] Enviando Comando Remoto (${action}):`, JSON.stringify(payload || {}));
  ws.send(JSON.stringify(message));

  pendingCommands.set(messageId, {
    charger_id: charger.id,
    charge_point_id: charger.charge_point_id,
    action,
    payload: payload || {},
    sent_at: nowIso()
  });

  addEvent({ chargePointId: charger.charge_point_id, direction: 'OUT', action, payload: payload || {} });
  res.json({ success: true, messageId, action, payload: payload || {} });
}

app.post('/api/chargers/:id/remote-start', (req, res) => {
  const { connectorId, idTag } = req.body;
  sendCommand(req.params.id, 'RemoteStartTransaction', { connectorId: connectorId || 1, idTag: idTag || 'ADMIN' }, res);
});

app.post('/api/chargers/:id/remote-stop', (req, res) => {
  const { transactionId } = req.body;
  if (transactionId === undefined || transactionId === null) {
    return res.status(400).json({ error: 'transactionId é obrigatório' });
  }
  sendCommand(req.params.id, 'RemoteStopTransaction', { transactionId }, res);
});

app.post('/api/chargers/:id/reset', (req, res) => {
  const { type } = req.body;
  sendCommand(req.params.id, 'Reset', { type: type || 'Soft' }, res);
});

app.post('/api/chargers/:id/get-configuration', (req, res) => {
  const { keys } = req.body;
  const payload = Array.isArray(keys) && keys.length > 0 ? { key: keys } : {};
  sendCommand(req.params.id, 'GetConfiguration', payload, res);
});

app.post('/api/chargers/:id/change-configuration', (req, res) => {
  const { key, value } = req.body;
  if (!key || value === undefined) return res.status(400).json({ error: 'key e value são obrigatórios' });
  sendCommand(req.params.id, 'ChangeConfiguration', { key, value: String(value) }, res);
});

app.post('/api/chargers/:id/change-availability', (req, res) => {
  const { connectorId, type } = req.body;
  sendCommand(req.params.id, 'ChangeAvailability', { connectorId: connectorId || 1, type: type || 'Operative' }, res);
});

app.post('/api/chargers/:id/unlock-connector', (req, res) => {
  const { connectorId } = req.body;
  sendCommand(req.params.id, 'UnlockConnector', { connectorId: connectorId || 1 }, res);
});

app.post('/api/chargers/:id/clear-cache', (req, res) => {
  sendCommand(req.params.id, 'ClearCache', {}, res);
});

app.post('/api/chargers/:id/trigger-message', (req, res) => {
  const { requestedMessage, connectorId } = req.body;
  const payload = { requestedMessage: requestedMessage || 'StatusNotification' };
  if (connectorId !== undefined && connectorId !== null) payload.connectorId = Number(connectorId);
  sendCommand(req.params.id, 'TriggerMessage', payload, res);
});

// MVP de ChargingProfile para teste futuro. Use com cuidado em carregador real.
app.post('/api/chargers/:id/set-charging-profile', (req, res) => {
  const { connectorId, chargingProfile } = req.body;
  if (!chargingProfile) return res.status(400).json({ error: 'chargingProfile é obrigatório' });
  sendCommand(req.params.id, 'SetChargingProfile', { connectorId: connectorId || 1, csChargingProfiles: chargingProfile }, res);
});

server.listen(PORT, () => {
  console.log(`[SERVER] Fotus Charge Backend rodando na porta ${PORT}`);
  console.log(`[WEBSOCKET] Servidor OCPP aguardando conexões em ws://localhost:${PORT}/ocpp/{chargePointId}`);
});
