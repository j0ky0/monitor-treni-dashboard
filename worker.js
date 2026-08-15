// ================================================================
// Cloudflare Worker: proxy + decodificatore GTFS-Realtime per ATAC
// FILTRATO sulle fermate e linee di interesse del pendolare
// ================================================================
// v8 (2026-08-15) — Novità: affollamento (occupancy_status) incrociato
// dal feed vehicle-positions e allegato alle corse di /trip-updates e
// /vettura/{matricola}, associando per vehicleId/vehicleLabel. Il feed
// trip-updates di per se' non contiene questo dato (vive solo in
// vehicle-positions), quindi serve un secondo fetch e un join lato Worker.
// Confermato con diagnostica dedicata: presente in ~59% dei veicoli
// (452/764 nel campione testato), assente per il resto — normale,
// dipende dal singolo mezzo/dispositivo di bordo.
//
// Scarica i feed pubblici di Roma Mobilita' (bloccati da CORS se
// letti direttamente dal browser), li decodifica dal formato
// binario Protocol Buffer, filtra solo le fermate/linee che ti
// interessano, e restituisce JSON pulito e leggero.
//
// Fonte dati ufficiale: https://romamobilita.it/sistemi-e-tecnologie/open-data/
//
// Endpoint disponibili:
//   /vehicle-positions  -> posizione GPS dei bus di interesse
//   /trip-updates       -> arrivi previsti con ritardo, identificativo
//                          vettura e affollamento, SOLO per le fermate
//                          configurate sotto
//   /vettura/{matricola} -> tracciamento di UNA vettura specifica su
//                          TUTTE le linee (non solo quelle configurate),
//                          con tutte le fermate future rimaste nella
//                          corsa e affollamento se disponibile.
// ================================================================

const URL_VEHICLE_POSITIONS = 'https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb';
const URL_TRIP_UPDATES = 'https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb';

// ----------------------------------------------------------------
// CONFIGURAZIONE: fermate e linee di interesse (aggiornato con le
// tratte andata + ritorno del foglio tratte_bus.xlsx). Usata anche
// da /vettura/{matricola} solo per dare un NOME leggibile alle
// fermate che riconosciamo — non per filtrare i risultati.
// ----------------------------------------------------------------
const FERMATE_INTERESSE = {
  '74299': { nome: "C.so d'Italia/P.za Fiume", linee: ['490', '495'] },
  '82007': { nome: 'Staz.ne Tiburtina (MB)', linee: ['490'] },
  '82005': { nome: 'Staz.ne Tiburtina (MB)', linee: ['495'] },
  '72661': { nome: 'Fiume', linee: ['80', '38'] },
  '72685': { nome: 'Libia (MB1)', linee: ['80', '38', '83'] },
  '84300': { nome: "Po/C.so d'Italia", linee: ['83'] },
  '74308': { nome: 'Fiume', linee: ['490', '495'] },
  '81874': { nome: 'Libia (MB1)', linee: ['83', '80', '38'] },
  '84299': { nome: "Po/C.so d'Italia", linee: ['83'] },
  '76852': { nome: 'Viterbo', linee: ['80', '38'] },
  '70441': { nome: 'Volturno/Gaeta', linee: ['38', '92', '360', '223'] },
  '70239': { nome: 'Indipendenza', linee: ['38', '92', '360', '223'] },
  '83173': { nome: 'Staz.ne Tiburtina (MB)', linee: ['62'] },
  '71555': { nome: 'Porta Pia', linee: ['62'] },
  '70237': { nome: 'Salaria/Aniene', linee: ['92', '360', '223'] },
  '73624': { nome: 'Fiume', linee: ['92'] },
  '70446': { nome: 'Fiume', linee: ['360', '223'] },
};
const STOP_IDS_INTERESSE = Object.keys(FERMATE_INTERESSE);
const LINEE_INTERESSE = new Set(
  Object.values(FERMATE_INTERESSE).flatMap(f => f.linee)
);

// Etichette leggibili per l'enum OccupancyStatus dello standard
// GTFS-Realtime (campo 9 di VehiclePosition).
const ETICHETTE_OCCUPANCY = {
  0: 'Vuoto',
  1: 'Posti disponibili',
  2: 'Pochi posti',
  3: 'Solo in piedi',
  4: 'Molto affollato',
  5: 'Pieno',
  6: 'Non accetta passeggeri',
  7: 'Dato non disponibile',
  8: 'Non salibile',
};

// ---------------- Decodificatore protobuf minimale ----------------

function readVarint(bytes, pos) {
  let result = 0n, shift = 0n;
  while (true) {
    const b = bytes[pos++];
    result |= BigInt(b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7n;
  }
  return [result, pos];
}

function decodeMessage(bytes, start, end) {
  const fields = {};
  let pos = start;
  while (pos < end) {
    const [tag, p1] = readVarint(bytes, pos);
    pos = p1;
    const fieldNumber = Number(tag >> 3n);
    const wireType = Number(tag & 7n);
    let value;
    if (wireType === 0) {
      const [v, p2] = readVarint(bytes, pos); value = v; pos = p2;
    } else if (wireType === 1) {
      value = bytes.slice(pos, pos + 8); pos += 8;
    } else if (wireType === 2) {
      const [len, p2] = readVarint(bytes, pos);
      pos = p2;
      value = bytes.slice(pos, pos + Number(len));
      pos += Number(len);
    } else if (wireType === 5) {
      value = bytes.slice(pos, pos + 4); pos += 4;
    } else {
      throw new Error(`Wire type non supportato: ${wireType} (campo ${fieldNumber})`);
    }
    if (!fields[fieldNumber]) fields[fieldNumber] = [];
    fields[fieldNumber].push(value);
  }
  return fields;
}

const dec = new TextDecoder('utf-8');
const toUtf8 = (bytes) => dec.decode(bytes);
const toFloat32LE = (bytes) => new DataView(bytes.buffer, bytes.byteOffset, 4).getFloat32(0, true);
const toInt = (bigintVal) => Number(bigintVal);
const toInt32Signed = (bigintVal) => Number(BigInt.asIntN(32, bigintVal));

// ---------------- Estrattori GTFS-Realtime ----------------

function estraiVehiclePosition(entityFields) {
  const id = entityFields[1] ? toUtf8(entityFields[1][0]) : null;
  const vBytes = entityFields[4] ? entityFields[4][0] : null;
  if (!vBytes) return null;

  const vp = decodeMessage(vBytes, 0, vBytes.length);
  const trip = vp[1] ? decodeMessage(vp[1][0], 0, vp[1][0].length) : {};
  const position = vp[2] ? decodeMessage(vp[2][0], 0, vp[2][0].length) : {};
  const vehicle = vp[8] ? decodeMessage(vp[8][0], 0, vp[8][0].length) : {};

  return {
    entityId: id,
    tripId: trip[1] ? toUtf8(trip[1][0]) : null,
    routeId: trip[5] ? toUtf8(trip[5][0]) : null,
    latitude: position[1] ? toFloat32LE(position[1][0]) : null,
    longitude: position[2] ? toFloat32LE(position[2][0]) : null,
    vehicleId: vehicle[1] ? toUtf8(vehicle[1][0]) : null,
    vehicleLabel: vehicle[2] ? toUtf8(vehicle[2][0]) : null,
    // NOTA: campo 4 di VehiclePosition e' "current_status" (enum 0/1/2),
    // non il vero timestamp (quello e' il campo 5, non ancora usato qui
    // per non rompere compatibilita' con la dashboard esistente).
    timestamp: vp[4] ? toInt(vp[4][0]) : null,
    occupancyStatus: vp[9] ? toInt(vp[9][0]) : null,
  };
}

// ----------------------------------------------------------------
// Costruisce una mappa vehicleId/vehicleLabel -> occupancyStatus
// leggendo l'intero feed vehicle-positions (non filtrato per linea,
// cosi' copre anche /vettura che cerca su tutte le linee).
// ----------------------------------------------------------------
async function costruisciMappaOccupancy() {
  const mappaPerId = new Map();
  const mappaPerLabel = new Map();
  try {
    const risposta = await fetch(URL_VEHICLE_POSITIONS);
    if (!risposta.ok) return { mappaPerId, mappaPerLabel };
    const buffer = await risposta.arrayBuffer();
    const bytes = new Uint8Array(buffer);
    const feedFields = decodeMessage(bytes, 0, bytes.length);
    const entities = feedFields[2] || [];

    for (const entityBytes of entities) {
      const entityFields = decodeMessage(entityBytes, 0, entityBytes.length);
      const estratto = estraiVehiclePosition(entityFields);
      if (!estratto || estratto.occupancyStatus === null) continue;
      if (estratto.vehicleId) mappaPerId.set(estratto.vehicleId, estratto.occupancyStatus);
      if (estratto.vehicleLabel) mappaPerLabel.set(estratto.vehicleLabel, estratto.occupancyStatus);
    }
  } catch {
    // Se il feed posizioni non e' raggiungibile, si prosegue senza
    // affollamento invece di far fallire tutta la richiesta.
  }
  return { mappaPerId, mappaPerLabel };
}

function estraiTripUpdate(entityFields) {
  const id = entityFields[1] ? toUtf8(entityFields[1][0]) : null;
  const tuBytes = entityFields[3] ? entityFields[3][0] : null;
  if (!tuBytes) return null;

  const tu = decodeMessage(tuBytes, 0, tuBytes.length);
  const trip = tu[1] ? decodeMessage(tu[1][0], 0, tu[1][0].length) : {};
  const vehicle = tu[3] ? decodeMessage(tu[3][0], 0, tu[3][0].length) : {};
  const routeId = trip[5] ? toUtf8(trip[5][0]) : null;

  // FILTRO PRECOCE: se la linea non ci interessa, non perdiamo
  // tempo a decodificare tutte le fermate di questo trip
  // (usato SOLO da /trip-updates, non da /vettura)
  if (routeId && LINEE_INTERESSE.size > 0 && !LINEE_INTERESSE.has(routeId)) {
    return null;
  }

  const stopTimeUpdatesGrezzi = (tu[2] || []).map((stuBytes) => {
    const stu = decodeMessage(stuBytes, 0, stuBytes.length);
    const arrival = stu[2] ? decodeMessage(stu[2][0], 0, stu[2][0].length) : {};
    const departure = stu[3] ? decodeMessage(stu[3][0], 0, stu[3][0].length) : {};
    return {
      stopSequence: stu[1] ? toInt(stu[1][0]) : null,
      stopId: stu[4] ? toUtf8(stu[4][0]) : null,
      arrivalDelay: arrival[1] ? toInt32Signed(arrival[1][0]) : null,
      arrivalTime: arrival[2] ? toInt(arrival[2][0]) : null,
      departureDelay: departure[1] ? toInt32Signed(departure[1][0]) : null,
      departureTime: departure[2] ? toInt(departure[2][0]) : null,
    };
  });

  // FILTRO: teniamo solo le fermate che ci interessano davvero
  const stopTimeUpdates = stopTimeUpdatesGrezzi.filter(
    stu => stu.stopId && STOP_IDS_INTERESSE.includes(stu.stopId)
  );

  if (stopTimeUpdates.length === 0) return null;

  return {
    entityId: id,
    tripId: trip[1] ? toUtf8(trip[1][0]) : null,
    routeId,
    vehicleId: vehicle[1] ? toUtf8(vehicle[1][0]) : null,
    vehicleLabel: vehicle[2] ? toUtf8(vehicle[2][0]) : null,
    timestamp: tu[4] ? toInt(tu[4][0]) : null,
    stopTimeUpdates,
  };
}

function decodeFeed(buffer, tipo) {
  const bytes = new Uint8Array(buffer);
  const feedFields = decodeMessage(bytes, 0, bytes.length);
  const entities = feedFields[2] || [];

  const risultati = [];
  for (const entityBytes of entities) {
    const entityFields = decodeMessage(entityBytes, 0, entityBytes.length);
    const estratto = tipo === 'vehicle'
      ? estraiVehiclePosition(entityFields)
      : estraiTripUpdate(entityFields);
    if (estratto) risultati.push(estratto);
  }
  return risultati;
}

// Filtro per vehicle-positions: teniamo solo i bus sulle linee di interesse
function filtraVehiclePositions(dati) {
  return dati.filter(v => v.routeId && LINEE_INTERESSE.has(v.routeId));
}

// Allega l'affollamento (se trovato) a ogni corsa di /trip-updates,
// associando per vehicleId prima, vehicleLabel come fallback.
function allegaOccupancy(dati, mappaOccupancy) {
  return dati.map(item => {
    let occupancyStatus = null;
    if (item.vehicleId && mappaOccupancy.mappaPerId.has(item.vehicleId)) {
      occupancyStatus = mappaOccupancy.mappaPerId.get(item.vehicleId);
    } else if (item.vehicleLabel && mappaOccupancy.mappaPerLabel.has(item.vehicleLabel)) {
      occupancyStatus = mappaOccupancy.mappaPerLabel.get(item.vehicleLabel);
    }
    return {
      ...item,
      occupancyStatus,
      occupancyLabel: occupancyStatus !== null ? (ETICHETTE_OCCUPANCY[occupancyStatus] || null) : null,
    };
  });
}

// ----------------------------------------------------------------
// Diagnostica mirata: restituisce la fetta HTML grezza intorno a UN
// treno specifico (per numero), utile se in futuro un altro campo
// si comporta in modo inatteso — evita di dover riguardare l'intera
// pagina a mano.
// ----------------------------------------------------------------
async function cercaVettura(matricola, mappaOccupancy) {
  const risposta = await fetch(URL_TRIP_UPDATES);
  if (!risposta.ok) {
    throw new Error(`Impossibile scaricare il feed trip-updates: HTTP ${risposta.status}`);
  }
  const buffer = await risposta.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  const feedFields = decodeMessage(bytes, 0, bytes.length);
  const entities = feedFields[2] || [];

  for (const entityBytes of entities) {
    const entityFields = decodeMessage(entityBytes, 0, entityBytes.length);
    const id = entityFields[1] ? toUtf8(entityFields[1][0]) : null;
    const tuBytes = entityFields[3] ? entityFields[3][0] : null;
    if (!tuBytes) continue;

    const tu = decodeMessage(tuBytes, 0, tuBytes.length);
    const trip = tu[1] ? decodeMessage(tu[1][0], 0, tu[1][0].length) : {};
    const vehicle = tu[3] ? decodeMessage(tu[3][0], 0, tu[3][0].length) : {};
    const vehicleId = vehicle[1] ? toUtf8(vehicle[1][0]) : null;
    const vehicleLabel = vehicle[2] ? toUtf8(vehicle[2][0]) : null;

    if (vehicleLabel === matricola || vehicleId === matricola) {
      const routeId = trip[5] ? toUtf8(trip[5][0]) : null;

      const stopTimeUpdates = (tu[2] || []).map((stuBytes) => {
        const stu = decodeMessage(stuBytes, 0, stuBytes.length);
        const arrival = stu[2] ? decodeMessage(stu[2][0], 0, stu[2][0].length) : {};
        const stopId = stu[4] ? toUtf8(stu[4][0]) : null;
        return {
          stopSequence: stu[1] ? toInt(stu[1][0]) : null,
          stopId,
          nomeFermata: FERMATE_INTERESSE[stopId] ? FERMATE_INTERESSE[stopId].nome : null,
          arrivalDelay: arrival[1] ? toInt32Signed(arrival[1][0]) : null,
          arrivalTime: arrival[2] ? toInt(arrival[2][0]) : null,
        };
      }).sort((a, b) => (a.stopSequence || 0) - (b.stopSequence || 0));

      let occupancyStatus = null;
      if (vehicleId && mappaOccupancy.mappaPerId.has(vehicleId)) {
        occupancyStatus = mappaOccupancy.mappaPerId.get(vehicleId);
      } else if (vehicleLabel && mappaOccupancy.mappaPerLabel.has(vehicleLabel)) {
        occupancyStatus = mappaOccupancy.mappaPerLabel.get(vehicleLabel);
      }

      return {
        entityId: id,
        tripId: trip[1] ? toUtf8(trip[1][0]) : null,
        routeId,
        vehicleId,
        vehicleLabel,
        occupancyStatus,
        occupancyLabel: occupancyStatus !== null ? (ETICHETTE_OCCUPANCY[occupancyStatus] || null) : null,
        stopTimeUpdates,
      };
    }
  }

  return null; // vettura non trovata nel feed attuale
}

// ---------------- Handler principale del Worker ----------------

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Content-Type': 'application/json; charset=utf-8',
};

function erroreJson(messaggio, status = 500) {
  return new Response(JSON.stringify({ errore: messaggio }), { status, headers: CORS_HEADERS });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }

    try {
      // ------------------------------------------------------------
      // Endpoint tracciamento vettura specifica: /vettura/{matricola}
      // Cache separata (chiave = URL completo, quindi per matricola)
      // ------------------------------------------------------------
      if (url.pathname.startsWith('/vettura/')) {
        const matricola = decodeURIComponent(url.pathname.split('/')[2] || '').trim();
        if (!matricola) {
          return erroreJson('Specifica una matricola: /vettura/{numero}', 400);
        }

        const cache = caches.default;
        const cacheKey = new Request(url.toString(), request);
        let rispostaCache = await cache.match(cacheKey);
        if (rispostaCache) {
          return rispostaCache;
        }

        const mappaOccupancy = await costruisciMappaOccupancy();
        const risultato = await cercaVettura(matricola, mappaOccupancy);
        if (!risultato) {
          return erroreJson('Vettura non trovata nel feed attuale (potrebbe essere fuori servizio, appena scesa dal servizio, o il feed non e\' ancora aggiornato)', 404);
        }

        const rispostaOk = new Response(JSON.stringify(risultato), {
          headers: { ...CORS_HEADERS, 'Cache-Control': 'public, max-age=60' },
        });
        ctx.waitUntil(cache.put(cacheKey, rispostaOk.clone()));
        return rispostaOk;
      }

      let sourceUrl, tipo;
      if (url.pathname === '/vehicle-positions') {
        sourceUrl = URL_VEHICLE_POSITIONS;
        tipo = 'vehicle';
      } else if (url.pathname === '/trip-updates') {
        sourceUrl = URL_TRIP_UPDATES;
        tipo = 'trip';
      } else {
        return new Response(JSON.stringify({
          errore: 'Path non valido. Usa /vehicle-positions, /trip-updates o /vettura/{matricola}',
          fermateConfigurate: FERMATE_INTERESSE,
        }), { status: 404, headers: CORS_HEADERS });
      }

      const cache = caches.default;
      const cacheKey = new Request(url.toString(), request);
      let risposta = await cache.match(cacheKey);
      if (risposta) {
        return risposta;
      }

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 8000);

      let rispostaFetch;
      try {
        rispostaFetch = await fetch(sourceUrl, { signal: controller.signal });
      } catch (fetchError) {
        clearTimeout(timeoutId);
        return new Response(JSON.stringify({
          errore: `Il server sorgente non ha risposto in tempo (timeout 8s): ${fetchError.message}`
        }), { status: 504, headers: CORS_HEADERS });
      }
      clearTimeout(timeoutId);

      if (!rispostaFetch.ok) {
        return new Response(JSON.stringify({
          errore: `Impossibile scaricare il feed sorgente: HTTP ${rispostaFetch.status}`
        }), { status: 502, headers: CORS_HEADERS });
      }

      const buffer = await rispostaFetch.arrayBuffer();
      let dati = decodeFeed(buffer, tipo);

      if (tipo === 'vehicle') {
        dati = filtraVehiclePositions(dati);
      } else if (tipo === 'trip') {
        // Incrocio con vehicle-positions per allegare l'affollamento
        // a ogni corsa filtrata (secondo fetch, sempre entro il timeout
        // complessivo della richiesta).
        const mappaOccupancy = await costruisciMappaOccupancy();
        dati = allegaOccupancy(dati, mappaOccupancy);
      }

      risposta = new Response(JSON.stringify({
        aggiornato: new Date().toISOString(),
        conteggio: dati.length,
        fermateConfigurate: FERMATE_INTERESSE,
        dati,
      }), {
        headers: {
          ...CORS_HEADERS,
          'Cache-Control': 'public, max-age=60',
        }
      });

      ctx.waitUntil(cache.put(cacheKey, risposta.clone()));
      return risposta;

    } catch (errore) {
      return new Response(JSON.stringify({
        errore: `Errore interno: ${errore.message}`
      }), { status: 500, headers: CORS_HEADERS });
    }
  },
};
