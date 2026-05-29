const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

// --- CONFIGURAÇÃO DE CORS ABERTA ---
app.use(cors({
  origin: '*', // Permite literalmente qualquer site
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json());

// --- IN-MEMORY STORAGE ---
let chargers = [];
let events = [];
let sessions = [];

// Mapeamento de conexões ativas (charge_point_id -> WebSocket)
const activeConnections = new Map();

// --- WEBSOCKET OCPP SERVER ---
wss.on('connection', (ws, req) => {
  // Validação estrita da rota: deve ser exatamente /ocpp/:chargePointId
  const urlMatch = req.url.match(/^\/ocpp\/(.+)$/);
  
  if (!urlMatch) {
    console.warn(`[OCPP] Conexão rejeitada: Rota inválida (${req.url}). Esperado /ocpp/:chargePointId`);
    ws.close(1008, 'Invalid route'); // 1008 Policy Violation
    return;
  }

  const chargePointId = urlMatch[1];
  console.log(`[OCPP] [${chargePointId}] Carregador conectado na rota ${req.url}`);
  
  activeConnections.set(chargePointId, ws);

  // Atualiza ou registra o carregador
  let charger = chargers.find(c => c.charge_point_id === chargePointId);
  if (charger) {
    charger.status = 'Online';
    charger.ultimo_heartbeat = new Date().toISOString();
  } else {
    charger = {
      id: crypto.randomUUID(),
      charge_point_id: chargePointId,
      fabricante: 'Desconhecido',
      modelo: 'Desconhecido',
      status: 'Online',
      ultimo_heartbeat: new Date().toISOString(),
      connectors: [{ id: crypto.randomUUID(), connector_number: 1, status: 'Available' }]
    };
    chargers.push(charger);
  }

  ws.on('message', (message) => {
    let parsed;
    
    // Tratamento de erro de JSON
    try {
      parsed = JSON.parse(message);
    } catch (err) {
      console.error(`[OCPP] [${chargePointId}] Erro de Parse JSON. Mensagem recebida:`, message.toString());
      return; // Ignora mensagens malformadas
    }
    
    try {
      // Mensagem OCPP CALL: [2, "UniqueId", "Action", { Payload }]
      if (Array.isArray(parsed) && parsed[0] === 2) {
        const [messageTypeId, messageId, action, payload] = parsed;
        
        console.log(`[OCPP] [${chargePointId}] Recebeu CALL: ${action}`, JSON.stringify(payload));

        // Registrar evento no histórico
        events.unshift({
          id: crypto.randomUUID(),
          charge_point_id: chargePointId,
          direction: 'IN',
          action: action,
          payload: JSON.stringify(payload),
          created_at: new Date().toISOString()
        });

        // Manter limite de eventos em memória
        if (events.length > 200) events.pop();

        // Auto-resposta básica (CallResult)
        let responsePayload = {};
        
        if (action === 'BootNotification') {
          console.log(`[OCPP] [${chargePointId}] Processando BootNotification (Vendor: ${payload.chargePointVendor}, Model: ${payload.chargePointModel})`);
          responsePayload = { status: 'Accepted', currentTime: new Date().toISOString(), interval: 300 };
          if (payload.chargePointVendor) charger.fabricante = payload.chargePointVendor;
          if (payload.chargePointModel) charger.modelo = payload.chargePointModel;
          
        } else if (action === 'Heartbeat') {
          console.log(`[OCPP] [${chargePointId}] Processando Heartbeat`);
          responsePayload = { currentTime: new Date().toISOString() };
          charger.ultimo_heartbeat = new Date().toISOString();
          
        } else if (action === 'StatusNotification') {
          console.log(`[OCPP] [${chargePointId}] Processando StatusNotification (Conector ${payload.connectorId}: ${payload.status})`);
          responsePayload = {};
          if (charger.connectors) {
            const conn = charger.connectors.find(c => c.connector_number === payload.connectorId);
            if (conn) conn.status = payload.status;
          }
          
        } else if (action === 'StartTransaction') {
          const transactionId = Math.floor(Math.random() * 1000000);
          console.log(`[OCPP] [${chargePointId}] Processando StartTransaction (Conector ${payload.connectorId}, Nova Transação: ${transactionId})`);
          
          responsePayload = {
            transactionId: transactionId,
            idTagInfo: { status: 'Accepted' }
          };
          
          // Criar nova sessão
          sessions.unshift({
            id: crypto.randomUUID(),
            charger_id: charger.id,
            charge_point_id: chargePointId,
            connector_id: payload.connectorId,
            transaction_id: transactionId,
            started_at: payload.timestamp || new Date().toISOString(),
            ended_at: null,
            energy_kwh: 0,
            status: 'Charging'
          });

          // Atualizar status do conector
          if (charger.connectors) {
            const conn = charger.connectors.find(c => c.connector_number === payload.connectorId);
            if (conn) conn.status = 'Occupied';
          }
          
        } else if (action === 'MeterValues') {
          responsePayload = {};
          const session = sessions.find(s => s.transaction_id === payload.transactionId && s.status === 'Charging');
          
          if (session && payload.meterValue && payload.meterValue.length > 0) {
            const meterValue = payload.meterValue[payload.meterValue.length - 1];
            const energyValue = meterValue.sampledValue.find(sv => sv.measurand === 'Energy.Active.Import.Register' || !sv.measurand);
            
            if (energyValue) {
              let value = parseFloat(energyValue.value);
              if (energyValue.unit === 'Wh') value = value / 1000;
              session.energy_kwh = value;
              console.log(`[OCPP] [${chargePointId}] Processando MeterValues (Transação ${payload.transactionId}, Energia Atual: ${value.toFixed(2)} kWh)`);
            } else {
              console.log(`[OCPP] [${chargePointId}] Processando MeterValues (Transação ${payload.transactionId}, Sem leitura de energia ativa)`);
            }
          } else {
            console.log(`[OCPP] [${chargePointId}] Processando MeterValues (Sessão não encontrada ou sem valores para Transação ${payload.transactionId})`);
          }
          
        } else if (action === 'StopTransaction') {
          console.log(`[OCPP] [${chargePointId}] Processando StopTransaction (Transação ${payload.transactionId})`);
          responsePayload = {
            idTagInfo: { status: 'Accepted' }
          };
          
          const session = sessions.find(s => s.transaction_id === payload.transactionId);
          if (session) {
            session.ended_at = payload.timestamp || new Date().toISOString();
            session.status = 'Completed';
            
            if (payload.meterStop !== undefined) {
              let finalEnergy = parseFloat(payload.meterStop);
              if (finalEnergy > 10000) finalEnergy = finalEnergy / 1000; 
              session.energy_kwh = finalEnergy;
              console.log(`[OCPP] [${chargePointId}] Transação ${payload.transactionId} finalizada com ${session.energy_kwh.toFixed(2)} kWh`);
            }
          }

          // Liberar conector
          if (charger.connectors) {
            const connectorId = session ? session.connector_id : 1;
            const conn = charger.connectors.find(c => c.connector_number === connectorId);
            if (conn) conn.status = 'Available';
          }
        } else {
          console.log(`[OCPP] [${chargePointId}] Ação não tratada especificamente: ${action}`);
        }

        // Enviar resposta CALLRESULT: [3, "UniqueId", { Payload }]
        console.log(`[OCPP] [${chargePointId}] Enviando CALLRESULT para ${action}:`, JSON.stringify(responsePayload));
        ws.send(JSON.stringify([3, messageId, responsePayload]));
        
      } else if (Array.isArray(parsed) && parsed[0] === 3) {
        // Mensagem OCPP CALLRESULT: [3, "UniqueId", { Payload }]
        console.log(`[OCPP] [${chargePointId}] Recebeu CALLRESULT (Id: ${parsed[1]}):`, JSON.stringify(parsed[2]));
      } else if (Array.isArray(parsed) && parsed[0] === 4) {
        // Mensagem OCPP CALLERROR: [4, "UniqueId", "ErrorCode", "ErrorDescription", { ErrorDetails }]
        console.error(`[OCPP] [${chargePointId}] Recebeu CALLERROR (Id: ${parsed[1]}):`, parsed[2], parsed[3]);
      } else {
        console.warn(`[OCPP] [${chargePointId}] Formato de mensagem OCPP desconhecido:`, parsed);
      }
    } catch (err) {
      console.error(`[OCPP] [${chargePointId}] Erro interno ao processar mensagem:`, err);
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

  res.json({
    chargers: { total, online, offline },
    connectors: { available: availableConnectors },
    sessions: { active: activeSessions, totalEnergy }
  });
});

app.get('/api/events', (req, res) => {
  res.json(events.slice(0, 50));
});

app.get('/api/chargers', (req, res) => {
  res.json(chargers);
});

app.get('/api/chargers/:id', (req, res) => {
  const charger = chargers.find(c => c.id === req.params.id);
  if (charger) {
    res.json(charger);
  } else {
    res.status(404).json({ error: 'Carregador não encontrado' });
  }
});

app.get('/api/logs/:id', (req, res) => {
  const charger = chargers.find(c => c.id === req.params.id);
  if (!charger) return res.status(404).json({ error: 'Carregador não encontrado' });
  
  const chargerEvents = events.filter(e => e.charge_point_id === charger.charge_point_id);
  res.json(chargerEvents.slice(0, 50));
});

app.get('/api/sessions', (req, res) => {
  res.json(sessions);
});

// --- COMMAND ROUTES (POST) ---

const sendCommand = (chargerId, action, payload, res) => {
  const charger = chargers.find(c => c.id === chargerId);
  if (!charger) return res.status(404).json({ error: 'Carregador não encontrado' });
  
  const ws = activeConnections.get(charger.charge_point_id);
  if (!ws) return res.status(400).json({ error: 'Carregador está offline' });

  const messageId = crypto.randomUUID();
  const message = [2, messageId, action, payload];
  
  console.log(`[OCPP] [${charger.charge_point_id}] Enviando Comando Remoto (${action}):`, JSON.stringify(payload));
  ws.send(JSON.stringify(message));
  
  events.unshift({
    id: crypto.randomUUID(),
    charge_point_id: charger.charge_point_id,
    direction: 'OUT',
    action: action,
    payload: JSON.stringify(payload),
    created_at: new Date().toISOString()
  });

  res.json({ success: true, messageId, action });
};

app.post('/api/chargers/:id/remote-start', (req, res) => {
  const { connectorId, idTag } = req.body;
  sendCommand(req.params.id, 'RemoteStartTransaction', { 
    connectorId: connectorId || 1, 
    idTag: idTag || 'ADMIN' 
  }, res);
});

app.post('/api/chargers/:id/remote-stop', (req, res) => {
  const { transactionId } = req.body;
  sendCommand(req.params.id, 'RemoteStopTransaction', { 
    transactionId: transactionId 
  }, res);
});

app.post('/api/chargers/:id/reset', (req, res) => {
  const { type } = req.body;
  sendCommand(req.params.id, 'Reset', { 
    type: type || 'Soft' 
  }, res);
});

// Inicialização do servidor
server.listen(PORT, () => {
  console.log(`[SERVER] Backend rodando na porta ${PORT}`);
  console.log(`[WEBSOCKET] Servidor OCPP aguardando conexões em ws://localhost:${PORT}/ocpp/{chargePointId}`);
});