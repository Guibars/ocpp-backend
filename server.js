const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// --- IN-MEMORY STORAGE ---
let chargers = [];
let events = [];
let sessions = [];

// Mapeamento de conexões ativas (charge_point_id -> WebSocket)
const activeConnections = new Map();

// --- WEBSOCKET OCPP SERVER ---
wss.on('connection', (ws, req) => {
  // A URL esperada é algo como ws://host:port/ocpp/WEMOB-001
  const urlParts = req.url.split('/');
  const chargePointId = urlParts[urlParts.length - 1];

  if (!chargePointId || chargePointId === 'ocpp') {
    console.log('[OCPP] Conexão rejeitada: ID do carregador não fornecido na URL.');
    ws.close();
    return;
  }

  console.log(`[OCPP] Carregador conectado: ${chargePointId}`);
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
    try {
      const parsed = JSON.parse(message);
      
      // Mensagem OCPP CALL: [2, "UniqueId", "Action", { Payload }]
      if (Array.isArray(parsed) && parsed[0] === 2) {
        const [messageTypeId, messageId, action, payload] = parsed;
        
        // Registrar evento
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
          responsePayload = { status: 'Accepted', currentTime: new Date().toISOString(), interval: 300 };
          if (payload.chargePointVendor) charger.fabricante = payload.chargePointVendor;
          if (payload.chargePointModel) charger.modelo = payload.chargePointModel;
        } else if (action === 'Heartbeat') {
          responsePayload = { currentTime: new Date().toISOString() };
          charger.ultimo_heartbeat = new Date().toISOString();
        } else if (action === 'StatusNotification') {
          responsePayload = {};
          if (charger.connectors) {
            const conn = charger.connectors.find(c => c.connector_number === payload.connectorId);
            if (conn) conn.status = payload.status;
          }
        }

        // Enviar resposta CALLRESULT: [3, "UniqueId", { Payload }]
        ws.send(JSON.stringify([3, messageId, responsePayload]));
      }
    } catch (err) {
      console.error(`[OCPP] Erro ao processar mensagem de ${chargePointId}:`, err);
    }
  });

  ws.on('close', () => {
    console.log(`[OCPP] Carregador desconectado: ${chargePointId}`);
    activeConnections.delete(chargePointId);
    if (charger) charger.status = 'Offline';
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
