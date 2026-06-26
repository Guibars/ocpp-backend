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
    console.log(`[WEBSOCKET] Subprotocolos compativeis: ${Array.from(OCPP_16_SUBPROTOCOLS).join(', ')}`);
  });
});
