const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
    credentials: false
  },
  transports: ['polling', 'websocket'],
  allowUpgrades: true,
  pingTimeout: 30000,
  pingInterval: 10000
});

// =====================================================
// SUPABASE - persistence only (not realtime)
// =====================================================
const SUPABASE_URL = 'https://lgydlidqobjdyssuanqw.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxneWRsaWRxb2JqZHlzc3VhbnF3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzk2ODU1NDAsImV4cCI6MjA5NTI2MTU0MH0.IxPMfmowznZbG2hIVbx7hTCv6-ZMdEHeoOMwNvavBMo';

const fetch = require('node-fetch');

async function sbQuery(method, table, body, params) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  if (params) Object.entries(params).forEach(([k,v]) => url.searchParams.set(k, v));
  const headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': `Bearer ${SUPABASE_KEY}`,
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
  };
  const res = await fetch(url.toString(), {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Supabase ${method} ${table}: ${err}`);
  }
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

async function saveRoom(room) {
  try {
    const data = {
      room_code: room.code,
      host_name: room.hostName,
      status: room.status,
      word_list: WORD_LIST,
      grid: room.grid,
      started_at: room.startedAt ? new Date(room.startedAt).toISOString() : null,
      ends_at: room.endsAt ? new Date(room.endsAt).toISOString() : null
    };
    // Upsert
    await sbQuery('POST', 'wordpuzzle_rooms', data);
  } catch(e) {
    console.log('saveRoom error (non-fatal):', e.message);
  }
}

async function updateRoomStatus(code, status) {
  try {
    await sbQuery('PATCH', `wordpuzzle_rooms?room_code=eq.${code}`, { status });
  } catch(e) {
    console.log('updateRoomStatus error (non-fatal):', e.message);
  }
}

async function deleteRoom(code) {
  try {
    await sbQuery('DELETE', `wordpuzzle_rooms?room_code=eq.${code}`, null);
  } catch(e) {
    console.log('deleteRoom error (non-fatal):', e.message);
  }
}

async function loadActiveRooms() {
  try {
    const rows = await sbQuery('GET', 'wordpuzzle_rooms', null, { 'status': 'in.(waiting,active)', 'select': '*' });
    if (!rows || !rows.length) return;
    const now = Date.now();
    for (const row of rows) {
      // Skip expired rooms
      if (row.ends_at && new Date(row.ends_at).getTime() < now) {
        await deleteRoom(row.room_code);
        continue;
      }
      // Restore into memory
      rooms[row.room_code] = {
        code: row.room_code,
        hostId: null, // host not connected yet
        hostName: row.host_name,
        status: row.status,
        grid: row.grid || [],
        placements: row.grid ? buildPlacementsFromGrid(row.grid, WORD_LIST) : {},
        players: {},
        startedAt: row.started_at ? new Date(row.started_at).getTime() : null,
        endsAt: row.ends_at ? new Date(row.ends_at).getTime() : null,
        timerInterval: null
      };
      // If room was active, restart timer
      if (row.status === 'active' && row.ends_at) {
        const endsAt = new Date(row.ends_at).getTime();
        const room = rooms[row.room_code];
        room.timerInterval = setInterval(() => {
          if (Date.now() >= endsAt) {
            clearInterval(room.timerInterval);
            endRoom(row.room_code, 'timeout');
          }
        }, 1000);
      }
      console.log(`Restored room ${row.room_code} (${row.status})`);
    }
  } catch(e) {
    console.log('loadActiveRooms error (non-fatal):', e.message);
  }
}

// =====================================================
// WORD LIST
// =====================================================
const WORD_LIST = [
  'CONSENSUS','VALIDATOR','GENLAYER','CONTRACT','TESTNET',
  'BRADBURY','FINALIZED','ACCEPTED','PYTHON','STAKING',
  'OPTIMISTIC','DELEGATED','APPEAL','GENVM','ORACLE',
  'SANDBOX','WEBHOOK','VOTING','ROLLUP','DISPATCH'
];

const GRID_SIZE = 15;
const GAME_DURATION = 300;
const DIRECTIONS = [
  [0,1],[0,-1],[1,0],[-1,0],
  [1,1],[1,-1],[-1,1],[-1,-1]
];

// =====================================================
// IN-MEMORY ROOMS
// =====================================================
const rooms = {};
const socketRoom = {};
const disconnectTimers = {}; // grace period timers for reconnecting players
const playerNames = {};      // socketId -> { name, roomCode } for reconnect matching

// =====================================================
// GRID HELPERS
// =====================================================
function generateGrid(words) {
  const grid = Array.from({ length: GRID_SIZE }, () => Array(GRID_SIZE).fill(''));
  const placements = {};
  const shuffled = [...words].sort(() => Math.random() - 0.5);
  for (const word of shuffled) {
    let placed = false, attempts = 0;
    while (!placed && attempts < 300) {
      attempts++;
      const [dr, dc] = DIRECTIONS[Math.floor(Math.random() * DIRECTIONS.length)];
      const r = Math.floor(Math.random() * GRID_SIZE);
      const c = Math.floor(Math.random() * GRID_SIZE);
      const cells = [];
      let valid = true;
      for (let i = 0; i < word.length; i++) {
        const nr = r + dr * i, nc = c + dc * i;
        if (nr < 0 || nr >= GRID_SIZE || nc < 0 || nc >= GRID_SIZE) { valid = false; break; }
        if (grid[nr][nc] !== '' && grid[nr][nc] !== word[i]) { valid = false; break; }
        cells.push([nr, nc]);
      }
      if (!valid) continue;
      cells.forEach(([nr, nc], i) => { grid[nr][nc] = word[i]; });
      placements[word] = cells.map(([nr, nc]) => ({ r: nr, c: nc }));
      placed = true;
    }
  }
  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  for (let r = 0; r < GRID_SIZE; r++)
    for (let c = 0; c < GRID_SIZE; c++)
      if (!grid[r][c]) grid[r][c] = letters[Math.floor(Math.random() * 26)];
  return { grid, placements };
}

function buildPlacementsFromGrid(grid, words) {
  const placements = {};
  for (const word of words) {
    outer: for (let r = 0; r < grid.length; r++) {
      for (let c = 0; c < grid[r].length; c++) {
        for (const [dr, dc] of DIRECTIONS) {
          let cells = [], ok = true;
          for (let i = 0; i < word.length; i++) {
            const nr = r + dr * i, nc = c + dc * i;
            if (nr < 0 || nr >= grid.length || nc < 0 || nc >= grid[0].length) { ok = false; break; }
            if (grid[nr][nc] !== word[i]) { ok = false; break; }
            cells.push({ r: nr, c: nc });
          }
          if (ok && cells.length === word.length) { placements[word] = cells; break outer; }
        }
      }
    }
  }
  return placements;
}

function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return Array.from({ length: 4 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}

// =====================================================
// ROOM HELPERS
// =====================================================
function getPublicPlayers(room) {
  return Object.entries(room.players).map(([id, p]) => ({
    id, name: p.name,
    wordsFound: p.wordsFound.length,
    lastWordAt: p.lastWordAt,
    finishedAt: p.finishedAt,
    isHost: id === room.hostId
  }));
}

function endRoom(code, reason) {
  const room = rooms[code];
  if (!room) return;
  if (room.timerInterval) { clearInterval(room.timerInterval); room.timerInterval = null; }
  room.status = 'finished';

  const finalScores = getPublicPlayers(room).map(p => ({
    name: p.name,
    wordsFound: p.wordsFound,
    timeTaken: p.finishedAt
      ? Math.floor((p.finishedAt - room.startedAt) / 1000)
      : p.lastWordAt
        ? Math.floor((p.lastWordAt - room.startedAt) / 1000)
        : GAME_DURATION
  })).sort((a, b) => b.wordsFound - a.wordsFound || a.timeTaken - b.timeTaken);

  io.to(code).emit('game_ended', { reason, finalScores });
  updateRoomStatus(code, 'finished');

  setTimeout(() => {
    if (rooms[code]) {
      Object.keys(rooms[code].players).forEach(sid => { delete socketRoom[sid]; });
      delete rooms[code];
    }
    deleteRoom(code);
  }, 30000);
}

function isNameTaken(name) {
  for (const code in rooms) {
    if (rooms[code].status === 'waiting') {
      for (const p of Object.values(rooms[code].players)) {
        if (p.name.toLowerCase() === name.toLowerCase()) return true;
      }
    }
  }
  return false;
}

// =====================================================
// SOCKET EVENTS
// =====================================================
io.on('connection', (socket) => {
  console.log('Connected:', socket.id);

  // Check if this is a reconnect - client sends their name and room code
  socket.on('reconnect_player', ({ name, code }) => {
    code = (code || '').trim().toUpperCase();
    const room = rooms[code];
    if (!room) return socket.emit('error', { message: 'Room no longer exists' });

    // Find disconnected player with this name
    const disconnectedEntry = Object.entries(room.players).find(
      ([sid, p]) => p.name === name && p.disconnected
    );

    if (!disconnectedEntry) {
      // Not found as disconnected - try joining fresh
      socket.emit('reconnect_failed', {});
      return;
    }

    const [oldSid, playerData] = disconnectedEntry;

    // Cancel grace period timer
    if (disconnectTimers[oldSid]) {
      clearTimeout(disconnectTimers[oldSid]);
      delete disconnectTimers[oldSid];
    }
    delete playerNames[oldSid];

    // Transfer player to new socket
    playerData.disconnected = false;
    room.players[socket.id] = playerData;
    delete room.players[oldSid];
    socketRoom[socket.id] = code;
    if (room.hostId === oldSid) room.hostId = socket.id;
    socket.join(code);

    const players = getPublicPlayers(room);
    const isNowHost = room.hostId === socket.id;
    // Send a single clean reconnected event with all state
    socket.emit('player_reconnected', {
      code,
      status: room.status,
      grid: room.grid,
      endsAt: room.endsAt,
      players,
      isHost: isNowHost
    });
    io.to(code).emit('player_joined', { players });
    console.log(`${name} reconnected to room ${code} (host: ${isNowHost})`);
  });

  socket.on('create_room', ({ name }) => {
    if (!name || !name.trim()) return socket.emit('error', { message: 'Enter your name first' });
    name = name.trim().slice(0, 20);
    if (isNameTaken(name)) return socket.emit('error', { message: 'Name already in use. Pick another.' });

    let code, attempts = 0;
    do { code = generateCode(); attempts++; } while (rooms[code] && attempts < 20);

    rooms[code] = {
      code, hostId: socket.id, hostName: name, status: 'waiting',
      grid: [], placements: {},
      players: { [socket.id]: { name, wordsFound: [], lastWordAt: null, finishedAt: null, joinedAt: Date.now() } },
      startedAt: null, endsAt: null, timerInterval: null
    };
    socketRoom[socket.id] = code;
    socket.join(code);
    saveRoom(rooms[code]);
    socket.emit('room_created', { code, players: getPublicPlayers(rooms[code]) });
    console.log(`Room ${code} created by ${name}`);
  });

  socket.on('join_room', ({ code, name }) => {
    code = (code || '').trim().toUpperCase();
    name = (name || '').trim().slice(0, 20);
    if (!code || code.length < 4) return socket.emit('error', { message: 'Enter a valid room code' });
    if (!name) return socket.emit('error', { message: 'Enter your name' });

    const room = rooms[code];
    if (!room) return socket.emit('error', { message: 'Room not found. Check the code.' });
    if (room.status === 'finished') return socket.emit('error', { message: 'That game already ended' });

    for (const p of Object.values(room.players)) {
      if (p.name.toLowerCase() === name.toLowerCase())
        return socket.emit('error', { message: 'Name already taken in this room' });
    }
    if (isNameTaken(name)) return socket.emit('error', { message: 'Name already in use. Pick another.' });

    room.players[socket.id] = { name, wordsFound: [], lastWordAt: null, finishedAt: null, joinedAt: Date.now() };
    socketRoom[socket.id] = code;
    socket.join(code);

    // If host had disconnected, reassign
    if (!room.hostId || !io.sockets.sockets.get(room.hostId)) {
      room.hostId = socket.id;
    }

    const players = getPublicPlayers(room);
    if (room.status === 'active') {
      socket.emit('room_joined', { code, status: 'active', grid: room.grid, endsAt: room.endsAt, players });
    } else {
      socket.emit('room_joined', { code, status: 'waiting', players });
    }
    io.to(code).emit('player_joined', { players });
    console.log(`${name} joined room ${code}`);
  });

  socket.on('start_game', () => {
    const code = socketRoom[socket.id];
    if (!code) return socket.emit('error', { message: 'Not in a room' });
    const room = rooms[code];
    if (!room) return socket.emit('error', { message: 'Room not found' });

    // Check by socket ID first, then by host name as fallback for reconnected hosts
    const player = room.players[socket.id];
    const isHost = room.hostId === socket.id ||
      (player && player.name === room.hostName);

    if (!isHost) return socket.emit('error', { message: 'Only the host can start' });

    // If matched by name but socket ID drifted, fix it
    if (room.hostId !== socket.id && player && player.name === room.hostName) {
      console.log(`Fixing host socket: ${room.hostId} -> ${socket.id}`);
      room.hostId = socket.id;
    }

    if (Object.keys(room.players).length < 2) return socket.emit('error', { message: 'Need at least 2 players to start' });
    if (room.status === 'active') return;

    const { grid, placements } = generateGrid(WORD_LIST);
    room.grid = grid;
    room.placements = placements;
    room.status = 'active';
    room.startedAt = Date.now();
    room.endsAt = Date.now() + GAME_DURATION * 1000;

    io.to(code).emit('game_started', { grid, endsAt: room.endsAt, players: getPublicPlayers(room) });
    saveRoom(room);

    room.timerInterval = setInterval(() => {
      if (Date.now() >= room.endsAt) {
        clearInterval(room.timerInterval);
        room.timerInterval = null;
        endRoom(code, 'timeout');
      }
    }, 1000);
    console.log(`Game started in room ${code}`);
  });

  socket.on('word_found', ({ word }) => {
    const code = socketRoom[socket.id];
    if (!code) return;
    const room = rooms[code];
    if (!room || room.status !== 'active') return;
    const player = room.players[socket.id];
    if (!player || !WORD_LIST.includes(word) || player.wordsFound.includes(word)) return;

    player.wordsFound.push(word);
    player.lastWordAt = Date.now();
    if (player.wordsFound.length === WORD_LIST.length) player.finishedAt = Date.now();

    io.to(code).emit('scores_updated', { players: getPublicPlayers(room) });

    if (Object.values(room.players).every(p => p.finishedAt !== null)) {
      clearInterval(room.timerInterval);
      endRoom(code, 'all_finished');
    }
  });

  socket.on('end_game', () => {
    const code = socketRoom[socket.id];
    if (!code || !rooms[code]) return;
    const room = rooms[code];
    const player = room.players[socket.id];
    const isHost = room.hostId === socket.id || (player && player.name === room.hostName);
    if (!isHost) return;
    if (room.hostId !== socket.id) room.hostId = socket.id;
    endRoom(code, 'host_ended');
  });

  socket.on('end_room', () => {
    const code = socketRoom[socket.id];
    if (!code || !rooms[code]) return;
    const room = rooms[code];
    const player = room.players[socket.id];
    const isHost = room.hostId === socket.id || (player && player.name === room.hostName);
    if (!isHost) return;
    io.to(code).emit('room_closed', { message: 'Host closed the room' });
    deleteRoom(code);
    Object.keys(room.players).forEach(sid => { delete socketRoom[sid]; });
    delete rooms[code];
  });

  socket.on('leave_room', () => handleLeave(socket));
  socket.on('disconnect', () => { console.log('Disconnected:', socket.id); handleLeave(socket); });

  function handleLeave(socket) {
    const code = socketRoom[socket.id];
    if (!code) return;
    const room = rooms[code];
    if (!room) { delete socketRoom[socket.id]; return; }

    const player = room.players[socket.id];
    const playerName = player ? player.name : 'Unknown';
    const isHost = socket.id === room.hostId;

    // Store info for potential reconnect
    playerNames[socket.id] = { name: playerName, roomCode: code, isHost };

    // Mark player as disconnected silently - no notification yet
    if (player) player.disconnected = true;
    delete socketRoom[socket.id];
    socket.leave(code);
    // Tell room the player is temporarily away (not fully left)
    io.to(code).emit('player_away', { playerName, players: getPublicPlayers(room) });

    // Grace period - if they reconnect within 30s, restore them
    disconnectTimers[socket.id] = setTimeout(() => {
      delete disconnectTimers[socket.id];
      delete playerNames[socket.id];

      // Now actually remove them
      const r = rooms[code];
      if (!r) return;
      delete r.players[socket.id];
      const remaining = Object.keys(r.players).filter(sid => !r.players[sid].disconnected).length;

      if (isHost && r.status === 'waiting') {
        io.to(code).emit('room_closed', { message: 'Host left the room' });
        deleteRoom(code);
        Object.keys(r.players).forEach(sid => { delete socketRoom[sid]; });
        delete rooms[code];
        return;
      }

      if (isHost && r.status === 'active') {
        // Find next non-disconnected player to be host
        const newHostId = Object.keys(r.players).find(sid => !r.players[sid].disconnected);
        if (newHostId) {
          r.hostId = newHostId;
          io.to(code).emit('host_changed', { newHostName: r.players[newHostId].name });
        }
      }

      if (Object.keys(r.players).length === 0) {
        if (r.timerInterval) clearInterval(r.timerInterval);
        deleteRoom(code);
        delete rooms[code];
        return;
      }

      io.to(code).emit('player_left', { playerName, players: getPublicPlayers(r) });
    }, isHost ? 10 * 60 * 1000 : 3 * 60 * 1000); // host: 10 min, player: 3 min
  }
});

// =====================================================
// HEALTH
// =====================================================
app.get('/', (req, res) => {
  res.json({ status: 'ok', rooms: Object.keys(rooms).length, uptime: Math.floor(process.uptime()) + 's' });
});
app.get('/health', (req, res) => res.json({ status: 'ok' }));
app.get('/ping', (req, res) => res.json({ pong: true, time: Date.now() }));

// =====================================================
// START
// =====================================================
const PORT = process.env.PORT || 3001;
server.listen(PORT, async () => {
  console.log(`GenPuzzles server running on port ${PORT}`);

  // Restore any active rooms from Supabase on startup
  await loadActiveRooms();

  // Self-ping every 4 minutes to prevent Render sleep
  const SELF_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
  setInterval(() => {
    const mod = SELF_URL.startsWith('https') ? require('https') : require('http');
    mod.get(SELF_URL + '/ping', () => {}).on('error', () => {});
  }, 4 * 60 * 1000);
});
