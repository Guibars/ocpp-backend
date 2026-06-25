const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, perMessageDeflate: false });

const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
const supabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false, autoRefreshToken: false }
    })
  : null;

app.use(cors());
app.use(express.json());

const activeConnections = new Map();
const pendingCommands = new Map();

function nowIso() {
  return new Date().toISOString();
}

function makeId() {
  return crypto.randomUUID();
}

function limitArray(arr, max = 500) {
  while (arr.length > max) arr.pop();
}

function parseJson(value, fallback = {}) {
  if (!value) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function parseChargerLocations() {
  if (!process.env.CHARGER_LOCATIONS) return {};
  try {
    const parsed = JSON.parse(process.env.CHARGER_LOCATIONS);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch (error) {
    console.error('[CONFIG] CHARGER_LOCATIONS precisa ser um JSON valido:', error.message);
    return {};
  }
}

function dbLog(action, error) {
  if (!error) return;
  console.warn(`[SUPABASE] ${action}:`, error.message || error);
}

async function dbSelect(table, buildQuery) {
  if (!supabase) return null;
  try {
    let query = supabase.from(table).select('*');
    if (buildQuery) query = buildQuery(query);
    const { data, error } = await query;
    if (error) {
      dbLog(`select ${table}`, error);
      return null;
    }
    return data || [];
  } catch (error) {
    dbLog(`select ${table}`, error);
    return null;
  }
}

async function dbInsert(table, payload) {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase.from(table).insert(payload).select().single();
    if (error) {
      dbLog(`insert ${table}`, error);
      return null;
    }
    return data || null;
  } catch (error) {
    dbLog(`insert ${table}`, error);
    return null;
  }
}

async function dbUpsert(table, payload, onConflict) {
  if (!supabase) return null;
  try {
    let query = supabase.from(table).upsert(payload, onConflict ? { onConflict } : undefined).select();
    if (!Array.isArray(payload)) query = query.single();
    const { data, error } = await query;
    if (error) {
      dbLog(`upsert ${table}`, error);
      return null;
    }
    return data || null;
  } catch (error) {
    dbLog(`upsert ${table}`, error);
    return null;
  }
}

let chargerLocations = parseChargerLocations();
let chargers = [];
let events = [];
let sessions = [];
let meterValues = [];
let commandResults = [];
let tariffs = [
  { id: makeId(), name: 'Madrugada', startHour: 0, endHour: 6, pricePerKwh: 1.5, active: true },
  { id: makeId(), name: 'Horario Comercial', startHour: 6, endHour: 18, pricePerKwh: 2.2, active: true },
  { id: makeId(), name: 'Horario de Ponta', startHour: 18, endHour: 22, pricePerKwh: 2.8, active: true },
  { id: makeId(), name: 'Noite', startHour: 22, endHour: 24, pricePerKwh: 1.9, active: true }
];

function tariffToDb(tariff) {
  return {
    id: tariff.id,
    name: tariff.name,
    start_hour: Number(tariff.startHour),
    end_hour: Number(tariff.endHour),
    price_per_kwh: Number(tariff.pricePerKwh),
    active: tariff.active !== false,
    updated_at: nowIso()
  };
}

function dbTariffToMemory(row) {
  return {
    id: row.id,
    name: row.name,
    startHour: Number(row.start_hour),
    endHour: Number(row.end_hour),
    pricePerKwh: Number(row.price_per_kwh),
    active: row.active !== false
  };
}

function connectorToDb(charger, connector) {
  return {
    id: connector.id,
    charger_id: charger.id,
    charge_point_id: charger.charge_point_id,
    connector_number: Number(connector.connector_number || 1),
    status: connector.status || 'Unknown',
    error_code: connector.error_code || 'NoError',
    info: connector.info || null,
    vendor_error_code: connector.vendor_error_code || null,
    timestamp: connector.timestamp || null,
    type: connector.type || 'CCS2',
    power_kw: Number(connector.powerKw || connector.power_kw || 50),
    price_per_kwh: connector.pricePerKwh ?? connector.price_per_kwh ?? null,
    updated_at: nowIso()
  };
}

function dbConnectorToMemory(row) {
  return {
    id: row.id,
    connector_number: Number(row.connector_number || 1),
    status: row.status || 'Unknown',
    error_code: row.error_code || 'NoError',
    info: row.info || null,
    vendor_error_code: row.vendor_error_code || null,
    timestamp: row.timestamp || null,
    type: row.type || 'CCS2',
    powerKw: row.power_kw !== null ? Number(row.power_kw) : undefined,
    pricePerKwh: row.price_per_kwh !== null ? Number(row.price_per_kwh) : undefined
  };
}

function chargerToDb(charger) {
  return {
    id: charger.id,
    charge_point_id: charger.charge_point_id,
    fabricante: charger.fabricante || 'Desconhecido',
    modelo: charger.modelo || 'Desconhecido',
    status: charger.status || 'Offline',
    ultimo_heartbeat: charger.ultimo_heartbeat || null,
    updated_at: nowIso()
  };
}

function sessionToDb(session) {
  return {
    id: session.id,
    charger_id: session.charger_id || null,
    charge_point_id: session.charge_point_id,
    connector_id: Number(session.connector_id || 1),
    transaction_id: session.transaction_id ?? null,
    id_tag: session.id_tag || null,
    started_at: session.started_at || null,
    ended_at: session.ended_at || null,
    meter_start_kwh: Number(session.meter_start_kwh || 0),
    meter_stop_kwh: session.meter_stop_kwh ?? null,
    energy_kwh: Number(session.energy_kwh || 0),
    current_power_kw: Number(session.current_power_kw || 0),
    price_per_kwh: Number(session.price_per_kwh || 0),
    current_cost: Number(session.current_cost || 0),
    tariff_name: session.tariff_name || null,
    status: session.status || 'Charging',
    updated_at: nowIso()
  };
}

function dbSessionToMemory(row) {
  return {
    id: row.id,
    charger_id: row.charger_id,
    charge_point_id: row.charge_point_id,
    connector_id: Number(row.connector_id || 1),
    transaction_id: row.transaction_id !== null ? Number(row.transaction_id) : null,
    id_tag: row.id_tag || null,
    started_at: row.started_at,
    ended_at: row.ended_at,
    meter_start_kwh: Number(row.meter_start_kwh || 0),
    meter_stop_kwh: row.meter_stop_kwh !== null ? Number(row.meter_stop_kwh) : null,
    energy_kwh: Number(row.energy_kwh || 0),
    current_power_kw: Number(row.current_power_kw || 0),
    price_per_kwh: Number(row.price_per_kwh || 0),
    current_cost: Number(row.current_cost || 0),
    tariff_name: row.tariff_name || null,
    status: row.status || 'Charging'
  };
}

function normalizeMeterForApi(record) {
  return {
    ...record,
    energia_kwh: record.energy_kwh,
    potencia_kw: record.power_kw,
    tensao_v: record.voltage,
    corrente_a: record.current
  };
}

async function persistTariff(tariff) {
  await dbUpsert('tariffs', tariffToDb(tariff), 'id');
}

async function persistChargerCascade(charger) {
  if (!supabase || !charger) return;
  const saved = await dbUpsert('chargers', chargerToDb(charger), 'charge_point_id');
  if (saved?.id) charger.id = saved.id;

  if (Array.isArray(charger.connectors)) {
    for (const connector of charger.connectors) {
      if (!connector.id) connector.id = makeId();
      await dbUpsert('connectors', connectorToDb(charger, connector), 'charger_id,connector_number');
    }
  }
}

async function persistLocation(chargePointId, location) {
  await dbUpsert('charger_locations', {
    charge_point_id: chargePointId,
    name: location.name || null,
    address: location.address || null,
    lat: Number(location.lat),
    lng: Number(location.lng),
    network: location.network || 'Fotus',
    connector_type: location.connectorType || location.connector_type || 'CCS2',
    power_kw: Number(location.powerKw || location.power_kw || 50),
    price_per_kwh: Number(location.pricePerKwh || location.price_per_kwh || 2.1),
    updated_at: nowIso()
  }, 'charge_point_id');
}

async function persistSession(session) {
  const conflict = session.transaction_id ? 'transaction_id' : 'id';
  await dbUpsert('charging_sessions', sessionToDb(session), conflict);
}

async function persistMeterValue(record) {
  await dbInsert('meter_values', {
    id: record.id,
    charge_point_id: record.charge_point_id,
    transaction_id: record.transaction_id,
    connector_id: record.connector_id,
    timestamp: record.timestamp,
    energy_kwh: record.energy_kwh,
    power_kw: record.power_kw,
    voltage: record.voltage,
    current: record.current,
    raw_payload: record.raw_payload || {}
  });
}

async function persistCommandResult(result) {
  await dbUpsert('command_results', {
    id: result.id,
    message_id: result.message_id,
    charge_point_id: result.charge_point_id,
    action: result.action,
    status: result.status,
    request_payload: result.request_payload || {},
    response_payload: result.response_payload || {},
    error_code: result.error_code || null,
    error_description: result.error_description || null,
    error_details: result.error_details || {},
    created_at: result.created_at || nowIso()
  }, 'message_id');
}

async function persistEvent(event) {
  await dbInsert('ocpp_events', {
    id: event.id,
    charge_point_id: event.charge_point_id,
    direction: event.direction,
    action: event.action,
    payload: parseJson(event.payload),
    created_at: event.created_at
  });
}

function addEvent({ chargePointId, direction, action, payload }) {
  const event = {
    id: makeId(),
    charge_point_id: chargePointId,
    direction,
    action,
    payload: typeof payload === 'string' ? payload : JSON.stringify(payload || {}),
    created_at: nowIso()
  };
  events.unshift(event);
  limitArray(events, 500);
  void persistEvent(event);
  return event;
}

async function loadInitialState() {
  if (!supabase) {
    console.log('[SUPABASE] Nao configurado. Usando memoria em tempo de execucao.');
    return;
  }

  console.log('[SUPABASE] Carregando dados persistidos...');

  const dbLocations = await dbSelect('charger_locations');
  if (dbLocations) {
    const savedLocations = {};
    dbLocations.forEach(row => {
      savedLocations[row.charge_point_id] = {
        name: row.name,
        address: row.address,
        lat: Number(row.lat),
        lng: Number(row.lng),
        network: row.network || 'Fotus',
        connectorType: row.connector_type || 'CCS2',
        powerKw: Number(row.power_kw || 50),
        pricePerKwh: Number(row.price_per_kwh || 2.1)
      };
    });
    chargerLocations = { ...chargerLocations, ...savedLocations };
  }

  for (const [chargePointId, location] of Object.entries(parseChargerLocations())) {
    if (location?.lat !== undefined && location?.lng !== undefined) {
      await persistLocation(chargePointId, location);
    }
  }

  const dbTariffs = await dbSelect('tariffs', query => query.order('start_hour', { ascending: true }));
  if (dbTariffs && dbTariffs.length > 0) {
    tariffs = dbTariffs.map(dbTariffToMemory);
  } else {
    for (const tariff of tariffs) await persistTariff(tariff);
  }

  const dbChargers = await dbSelect('chargers', query => query.order('updated_at', { ascending: false }));
  const dbConnectors = await dbSelect('connectors');
  if (dbChargers) {
    chargers = dbChargers.map(row => ({
      id: row.id,
      charge_point_id: row.charge_point_id,
      fabricante: row.fabricante || 'Desconhecido',
      modelo: row.modelo || 'Desconhecido',
      status: row.status || 'Offline',
      ultimo_heartbeat: row.ultimo_heartbeat,
      connectors: (dbConnectors || [])
        .filter(connector => connector.charger_id === row.id)
        .map(dbConnectorToMemory)
    }));
  }

  const dbSessions = await dbSelect('charging_sessions', query => query.order('started_at', { ascending: false }).limit(300));
  if (dbSessions) sessions = dbSessions.map(dbSessionToMemory);

  const dbMeters = await dbSelect('meter_values', query => query.order('created_at', { ascending: false }).limit(1000));
  if (dbMeters) {
    meterValues = dbMeters.map(row => ({
      id: row.id,
      charge_point_id: row.charge_point_id,
      transaction_id: row.transaction_id !== null ? Number(row.transaction_id) : null,
      connector_id: row.connector_id,
      timestamp: row.timestamp,
      energy_kwh: row.energy_kwh !== null ? Number(row.energy_kwh) : null,
      power_kw: row.power_kw !== null ? Number(row.power_kw) : null,
      voltage: row.voltage !== null ? Number(row.voltage) : null,
      current: row.current !== null ? Number(row.current) : null,
      raw_payload: row.raw_payload || {},
      created_at: row.created_at
    }));
  }

  const dbCommands = await dbSelect('command_results', query => query.order('created_at', { ascending: false }).limit(300));
  if (dbCommands) commandResults = dbCommands;

  const dbEvents = await dbSelect('ocpp_events', query => query.order('created_at', { ascending: false }).limit(500));
  if (dbEvents) {
    events = dbEvents.map(row => ({
      id: row.id,
      charge_point_id: row.charge_point_id,
      direction: row.direction,
      action: row.action,
      payload: JSON.stringify(row.payload || {}),
      created_at: row.created_at
    }));
  }

  console.log(`[SUPABASE] Pronto. ${chargers.length} carregadores, ${sessions.length} sessoes, ${events.length} eventos.`);
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
  return {
    tariff,
    price_per_kwh: pricePerKwh,
    current_cost: Number((Number(energyKwh || 0) * pricePerKwh).toFixed(2))
  };
}

function getOrCreateConnector(charger, connectorId) {
  const number = Number(connectorId || 1);
  if (!charger.connectors) charger.connectors = [];
  let connector = charger.connectors.find(c => c.connector_number === number);
  if (!connector) {
    connector = {
      id: makeId(),
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

function findCharger(chargerId) {
  return chargers.find(c => c.id === chargerId || c.charge_point_id === chargerId);
}

function findActiveSessionForCharger(chargerId) {
  const charger = findCharger(chargerId);
  if (!charger) return null;
  return sessions.find(s =>
    (s.charger_id === charger.id || s.charge_point_id === charger.charge_point_id) &&
    (s.status === 'Charging' || s.ended_at === null)
  ) || null;
}

function getChargerLocation(charger) {
  if (!charger) return null;
  return chargerLocations[charger.charge_point_id] || chargerLocations[charger.id] || charger.location || null;
}

function connectorIsAvailable(connector) {
  return String(connector?.status || '').toLowerCase() === 'available';
}

function normalizePublicCharger(charger) {
  const location = getChargerLocation(charger);
  if (!location || location.lat === undefined || location.lng === undefined) return null;

  const connectors = Array.isArray(charger.connectors) && charger.connectors.length > 0
    ? charger.connectors
    : [{ connector_number: 1, status: charger.status === 'Online' ? 'Available' : 'Unavailable' }];

  return {
    id: charger.charge_point_id,
    chargerId: charger.id,
    name: location.name || `Carregador ${charger.charge_point_id}`,
    address: location.address || 'Endereco nao informado',
    lat: Number(location.lat),
    lng: Number(location.lng),
    network: location.network || 'Fotus',
    status: charger.status,
    online: charger.status === 'Online',
    lastSeen: charger.ultimo_heartbeat || null,
    connectors: connectors.map(connector => ({
      id: connector.id,
      connectorId: connector.connector_number || 1,
      type: location.connectorType || connector.type || 'CCS2',
      powerKw: Number(location.powerKw || connector.powerKw || 50),
      status: charger.status === 'Online' ? connector.status : 'Offline',
      available: charger.status === 'Online' && connectorIsAvailable(connector),
      pricePerKwh: Number(location.pricePerKwh || connector.pricePerKwh || 2.1)
    }))
  };
}

function sendCallResult(ws, chargePointId, messageId, action, payload) {
  console.log(`[OCPP] [${chargePointId}] Enviando CALLRESULT para ${action}:`, JSON.stringify(payload));
  ws.send(JSON.stringify([3, messageId, payload]));
}

function sendCallError(ws, chargePointId, messageId, errorCode, errorDescription, details = {}) {
  console.error(`[OCPP] [${chargePointId}] Enviando CALLERROR: ${errorCode} - ${errorDescription}`);
  ws.send(JSON.stringify([4, messageId, errorCode, errorDescription, details]));
}

function sendCommand(chargerId, action, payload, res) {
  const charger = findCharger(chargerId);
  if (!charger) return res.status(404).json({ error: 'Carregador nao encontrado' });

  const ws = activeConnections.get(charger.charge_point_id);
  if (!ws || ws.readyState !== 1) return res.status(400).json({ error: 'Carregador esta offline' });

  const messageId = makeId();
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

wss.on('connection', async (ws, req) => {
  const urlMatch = req.url.match(/^\/ocpp\/(.+)$/);
  if (!urlMatch) {
    console.warn(`[OCPP] Conexao rejeitada: rota invalida (${req.url}). Esperado /ocpp/:chargePointId`);
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
      id: makeId(),
      charge_point_id: chargePointId,
      fabricante: 'Desconhecido',
      modelo: 'Desconhecido',
      status: 'Online',
      ultimo_heartbeat: nowIso(),
      connectors: [{
        id: makeId(),
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
  await persistChargerCascade(charger);

  ws.on('message', async (message) => {
    let parsed;
    try {
      parsed = JSON.parse(message);
    } catch (err) {
      console.error(`[OCPP] [${chargePointId}] Erro de parse JSON:`, message.toString());
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
          await persistChargerCascade(charger);

        } else if (action === 'Heartbeat') {
          responsePayload = { currentTime: nowIso() };
          charger.status = 'Online';
          charger.ultimo_heartbeat = nowIso();
          await persistChargerCascade(charger);

        } else if (action === 'Authorize') {
          responsePayload = { idTagInfo: { status: 'Accepted' } };

        } else if (action === 'StatusNotification') {
          responsePayload = {};
          const connector = getOrCreateConnector(charger, payload.connectorId || 1);
          connector.status = payload.status || connector.status;
          connector.error_code = payload.errorCode || 'NoError';
          connector.info = payload.info || null;
          connector.vendor_error_code = payload.vendorErrorCode || null;
          connector.timestamp = payload.timestamp || nowIso();
          charger.status = 'Online';
          charger.ultimo_heartbeat = nowIso();
          await persistChargerCascade(charger);

        } else if (action === 'StartTransaction') {
          const transactionId = Math.floor(Math.random() * 1000000);
          const meterStartKwh = normalizeEnergyToKwh(payload.meterStart, payload.unit) || 0;
          const costInfo = calculateCost(0);

          responsePayload = { transactionId, idTagInfo: { status: 'Accepted' } };

          const session = {
            id: makeId(),
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
          };

          sessions.unshift(session);
          limitArray(sessions, 300);
          await persistSession(session);

          const connector = getOrCreateConnector(charger, payload.connectorId || 1);
          connector.status = 'Occupied';
          await persistChargerCascade(charger);

        } else if (action === 'MeterValues') {
          responsePayload = {};
          const session = sessions.find(s => Number(s.transaction_id) === Number(payload.transactionId) && s.status === 'Charging');

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
              id: makeId(),
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
            await persistMeterValue(record);

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
              await persistSession(session);
            }
          }

        } else if (action === 'StopTransaction') {
          responsePayload = { idTagInfo: { status: 'Accepted' } };
          const session = sessions.find(s => Number(s.transaction_id) === Number(payload.transactionId));

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
            await persistSession(session);
          }

          const connectorId = session ? session.connector_id : 1;
          const connector = getOrCreateConnector(charger, connectorId);
          connector.status = 'Available';
          await persistChargerCascade(charger);

        } else {
          console.log(`[OCPP] [${chargePointId}] Acao nao tratada especificamente: ${action}. Respondendo vazio.`);
          responsePayload = {};
        }

        sendCallResult(ws, chargePointId, messageId, action, responsePayload);

      } else if (Array.isArray(parsed) && parsed[0] === 3) {
        const [, messageId, payload] = parsed;
        const pending = pendingCommands.get(messageId);
        console.log(`[OCPP] [${chargePointId}] Recebeu CALLRESULT (${messageId}):`, JSON.stringify(payload));

        const commandResult = {
          id: makeId(),
          message_id: messageId,
          charge_point_id: chargePointId,
          action: pending ? pending.action : 'Unknown',
          status: payload?.status || 'Accepted',
          request_payload: pending ? pending.payload : null,
          response_payload: payload,
          created_at: nowIso()
        };
        commandResults.unshift(commandResult);
        limitArray(commandResults, 300);
        await persistCommandResult(commandResult);

        addEvent({ chargePointId, direction: 'IN', action: pending ? `${pending.action}Result` : 'CALLRESULT', payload });
        pendingCommands.delete(messageId);

      } else if (Array.isArray(parsed) && parsed[0] === 4) {
        const [, messageId, errorCode, errorDescription, errorDetails] = parsed;
        const pending = pendingCommands.get(messageId);
        console.error(`[OCPP] [${chargePointId}] Recebeu CALLERROR (${messageId}):`, errorCode, errorDescription);

        const commandResult = {
          id: makeId(),
          message_id: messageId,
          charge_point_id: chargePointId,
          action: pending ? pending.action : 'Unknown',
          status: 'Error',
          request_payload: pending ? pending.payload : null,
          response_payload: null,
          error_code: errorCode,
          error_description: errorDescription,
          error_details: errorDetails || {},
          created_at: nowIso()
        };
        commandResults.unshift(commandResult);
        limitArray(commandResults, 300);
        await persistCommandResult(commandResult);

        addEvent({ chargePointId, direction: 'IN', action: pending ? `${pending.action}Error` : 'CALLERROR', payload: { errorCode, errorDescription, errorDetails } });
        pendingCommands.delete(messageId);

      } else {
        console.warn(`[OCPP] [${chargePointId}] Formato desconhecido:`, parsed);
      }
    } catch (err) {
      console.error(`[OCPP] [${chargePointId}] Erro interno ao processar mensagem:`, err);
      if (Array.isArray(parsed) && parsed[0] === 2) {
        sendCallError(ws, chargePointId, parsed[1], 'InternalError', err.message || 'Internal server error');
      }
    }
  });

  ws.on('close', async (code, reason) => {
    console.log(`[OCPP] [${chargePointId}] Carregador desconectado (${code}, ${reason || 'N/A'})`);
    activeConnections.delete(chargePointId);
    if (charger) {
      charger.status = 'Offline';
      await persistChargerCascade(charger);
    }
  });

  ws.on('error', (error) => {
    console.error(`[OCPP] [${chargePointId}] Erro WebSocket:`, error);
  });
});

app.get('/', (req, res) => {
  res.json({
    name: 'Fotus Charge OCPP Backend',
    status: 'online',
    database: supabase ? 'supabase' : 'memory',
    websocket: '/ocpp/{chargePointId}',
    timestamp: nowIso()
  });
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', database: supabase ? 'supabase' : 'memory', timestamp: nowIso() });
});

app.get('/api/dashboard-stats', (req, res) => {
  const total = chargers.length;
  const online = chargers.filter(c => c.status === 'Online').length;
  const offline = total - online;
  let availableConnectors = 0;
  chargers.forEach(c => {
    if (c.connectors) availableConnectors += c.connectors.filter(conn => conn.status === 'Available').length;
  });

  const activeSessions = sessions.filter(s => s.status === 'Charging').length;
  const totalEnergy = sessions.reduce((acc, s) => acc + Number(s.energy_kwh || 0), 0);
  const totalRevenue = sessions.reduce((acc, s) => acc + Number(s.current_cost || 0), 0);

  res.json({
    chargers: { total, online, offline },
    connectors: { available: availableConnectors },
    sessions: {
      active: activeSessions,
      totalEnergy: Number(totalEnergy.toFixed(3)),
      totalRevenue: Number(totalRevenue.toFixed(2))
    },
    tariff: getCurrentTariff(),
    chargersOnline: online,
    chargersOffline: offline,
    activeSessions,
    totalEnergyConsumed: Number(totalEnergy.toFixed(3))
  });
});

app.get('/api/events', (req, res) => {
  res.json(events.slice(0, Number(req.query.limit || 50)));
});

app.get('/chargers', (req, res) => {
  const availableOnly = String(req.query.available || 'true') !== 'false';
  const mappedChargers = chargers
    .map(normalizePublicCharger)
    .filter(Boolean)
    .filter(charger => !availableOnly || charger.connectors.some(connector => connector.available));
  res.json({ chargers: mappedChargers });
});

app.get('/api/public-chargers', (req, res) => {
  res.json({ chargers: chargers.map(normalizePublicCharger).filter(Boolean) });
});

app.post('/api/chargers/:id/location', async (req, res) => {
  const charger = findCharger(req.params.id) || { charge_point_id: req.params.id };
  const { name, address, lat, lng, network, connectorType, powerKw, pricePerKwh } = req.body;
  if (lat === undefined || lng === undefined) return res.status(400).json({ error: 'lat e lng sao obrigatorios' });

  const location = {
    name,
    address,
    lat: Number(lat),
    lng: Number(lng),
    network: network || 'Fotus',
    connectorType: connectorType || 'CCS2',
    powerKw: Number(powerKw || 50),
    pricePerKwh: Number(pricePerKwh || 2.1)
  };
  chargerLocations[charger.charge_point_id] = location;
  await persistLocation(charger.charge_point_id, location);
  res.json({ success: true, charger: normalizePublicCharger(charger) });
});

app.get('/api/chargers', (req, res) => res.json(chargers));

app.get('/api/chargers/:id', (req, res) => {
  const charger = findCharger(req.params.id);
  if (charger) return res.json(charger);
  res.status(404).json({ error: 'Carregador nao encontrado' });
});

app.get('/api/logs/:id', (req, res) => {
  const charger = findCharger(req.params.id);
  if (!charger) return res.status(404).json({ error: 'Carregador nao encontrado' });
  const chargerEvents = events.filter(e => e.charge_point_id === charger.charge_point_id);
  res.json(chargerEvents.slice(0, Number(req.query.limit || 50)));
});

app.get('/api/sessions', (req, res) => res.json(sessions));

app.get('/api/meter-values', (req, res) => {
  const { transactionId, chargePointId } = req.query;
  let data = meterValues;
  if (transactionId) data = data.filter(m => String(m.transaction_id) === String(transactionId));
  if (chargePointId) data = data.filter(m => m.charge_point_id === chargePointId);
  res.json(data.slice(0, Number(req.query.limit || 100)).map(normalizeMeterForApi));
});

app.get('/api/command-results', (req, res) => {
  res.json(commandResults.slice(0, Number(req.query.limit || 100)));
});

app.get('/api/command-results/:messageId', (req, res) => {
  const result = commandResults.find(item => item.message_id === req.params.messageId || item.id === req.params.messageId);
  if (!result) return res.status(404).json({ error: 'Resultado de comando nao encontrado' });
  res.json(result);
});

app.get('/api/tariffs', (req, res) => res.json(tariffs));

app.get('/api/tariffs/current', (req, res) => res.json(getCurrentTariff()));

app.get('/api/tariffs/estimate', (req, res) => {
  const energyKwh = Number(req.query.energyKwh || 0);
  if (!Number.isFinite(energyKwh) || energyKwh < 0) return res.status(400).json({ error: 'energyKwh invalido' });
  const costInfo = calculateCost(energyKwh);
  res.json({
    energyKwh,
    tariffName: costInfo.tariff?.name || null,
    pricePerKwh: costInfo.price_per_kwh,
    estimatedCost: costInfo.current_cost,
    tariff: costInfo.tariff
  });
});

app.patch('/api/tariffs/current', async (req, res) => {
  const { pricePerKwh, name } = req.body;
  const tariff = getCurrentTariff();
  if (!tariff) return res.status(404).json({ error: 'Tarifa corrente nao encontrada' });
  if (pricePerKwh !== undefined) tariff.pricePerKwh = Number(pricePerKwh);
  if (name) tariff.name = String(name);
  await persistTariff(tariff);
  res.json({ success: true, ...tariff });
});

app.post('/api/tariffs', async (req, res) => {
  const { name, startHour, endHour, pricePerKwh, active } = req.body;
  if (!name || startHour === undefined || endHour === undefined || pricePerKwh === undefined) {
    return res.status(400).json({ error: 'Campos obrigatorios: name, startHour, endHour, pricePerKwh' });
  }
  const tariff = { id: makeId(), name, startHour: Number(startHour), endHour: Number(endHour), pricePerKwh: Number(pricePerKwh), active: active !== false };
  tariffs.push(tariff);
  await persistTariff(tariff);
  res.status(201).json(tariff);
});

app.put('/api/tariffs/:id', async (req, res) => {
  const tariff = tariffs.find(t => t.id === req.params.id);
  if (!tariff) return res.status(404).json({ error: 'Tarifa nao encontrada' });
  const { name, startHour, endHour, pricePerKwh, active } = req.body;
  if (name !== undefined) tariff.name = name;
  if (startHour !== undefined) tariff.startHour = Number(startHour);
  if (endHour !== undefined) tariff.endHour = Number(endHour);
  if (pricePerKwh !== undefined) tariff.pricePerKwh = Number(pricePerKwh);
  if (active !== undefined) tariff.active = Boolean(active);
  await persistTariff(tariff);
  res.json(tariff);
});

app.delete('/api/tariffs/:id', async (req, res) => {
  const before = tariffs.length;
  tariffs = tariffs.filter(t => t.id !== req.params.id);
  if (tariffs.length === before) return res.status(404).json({ error: 'Tarifa nao encontrada' });
  if (supabase) {
    const { error } = await supabase.from('tariffs').delete().eq('id', req.params.id);
    if (error) dbLog('delete tariffs', error);
  }
  res.json({ success: true });
});

app.post('/api/chargers/:id/remote-start', (req, res) => {
  const { connectorId, idTag } = req.body;
  sendCommand(req.params.id, 'RemoteStartTransaction', { connectorId: connectorId || 1, idTag: idTag || 'ADMIN' }, res);
});

app.post('/api/chargers/:id/remote-stop', (req, res) => {
  let { transactionId } = req.body;
  if (transactionId === undefined || transactionId === null) {
    const activeSession = findActiveSessionForCharger(req.params.id);
    if (activeSession) transactionId = activeSession.transaction_id;
  }
  if (transactionId === undefined || transactionId === null) return res.status(400).json({ error: 'transactionId e obrigatorio' });
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
  if (!key || value === undefined) return res.status(400).json({ error: 'key e value sao obrigatorios' });
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

app.post('/api/chargers/:id/clear-cache', (req, res) => sendCommand(req.params.id, 'ClearCache', {}, res));

app.post('/api/chargers/:id/trigger-message', (req, res) => {
  const { requestedMessage, connectorId } = req.body;
  const payload = { requestedMessage: requestedMessage || 'StatusNotification' };
  if (connectorId !== undefined && connectorId !== null) payload.connectorId = Number(connectorId);
  sendCommand(req.params.id, 'TriggerMessage', payload, res);
});

app.post('/api/chargers/:id/set-charging-profile', (req, res) => {
  const { connectorId, chargingProfile, csChargingProfiles } = req.body;
  const profile = chargingProfile || csChargingProfiles;
  if (!profile) return res.status(400).json({ error: 'chargingProfile e obrigatorio' });
  sendCommand(req.params.id, 'SetChargingProfile', { connectorId: connectorId || 1, csChargingProfiles: profile }, res);
});

loadInitialState().finally(() => {
  server.listen(PORT, () => {
    console.log(`[SERVER] Fotus Charge Backend rodando na porta ${PORT}`);
    console.log(`[DATABASE] ${supabase ? 'Supabase conectado' : 'Memoria local'}`);
    console.log(`[WEBSOCKET] Servidor OCPP aguardando conexoes em ws://localhost:${PORT}/ocpp/{chargePointId}`);
  });
});
