// Name: IndexedDB Storage
// ID: indexeddbstorage
// Description: Store large amounts of data persistently using IndexedDB. Like local storage, but with much more space and binary-safe. Supports namespaces so multiple projects can keep their data separate.
// By: Sakura Matsumoto
// License: MIT

(function (Scratch) {
  "use strict";

  if (!Scratch.extensions.unsandboxed) {
    throw new Error("IndexedDB Storage must be run unsandboxed");
  }

  const EXTENSION_ID = "indexeddbstorage";
  const DB_NAME_PREFIX = "extensions.turbowarp.org/indexeddb-storage:";
  const CHANNEL_NAME_PREFIX = "extensions.turbowarp.org/indexeddb-storage-channel:";
  const STORE_NAME = "data";
  const DEFAULT_MIME = "application/octet-stream";

  // ---------------------------------------------------------------------------
  // Namespace management
  //   A namespace maps 1:1 to a IndexedDB database and a BroadcastChannel.
  //   The current namespace is stored on Scratch.vm.runtime.extensionStorage
  //   so that it survives PROJECT_LOADED / green-flag runs and gets restored
  //   when the project is reopened.
  // ---------------------------------------------------------------------------

  const getNamespace = () =>
    Scratch.vm.runtime.extensionStorage[EXTENSION_ID]?.namespace;

  const setNamespace = (newNamespace) => {
    // Close the old connection & BroadcastChannel; the next op will reopen.
    closeDB();
    closeChannel();

    Scratch.vm.runtime.extensionStorage[EXTENSION_ID] = {
      namespace: newNamespace,
    };

    // Re-open the channel immediately so that "when another window changes
    // storage" is fired as soon as the namespace is set.
    ensureChannel();

    if (Scratch.vm.extensionManager.isExtensionLoaded(EXTENSION_ID)) {
      Scratch.vm.extensionManager.refreshBlocks(EXTENSION_ID);
    }
  };

  // 16 hex chars = 16^16 possible namespaces; plenty.
  const generateRandomNamespace = () => {
    const soup = "0123456789abcdef";
    let id = "";
    for (let i = 0; i < 16; i++) {
      id += soup[Math.floor(Math.random() * soup.length)];
    }
    return id;
  };

  const prepareInitialNamespace = () => {
    if (!getNamespace()) {
      setNamespace(generateRandomNamespace());
    } else {
      ensureChannel();
    }
  };

  // ---------------------------------------------------------------------------
  // IndexedDB plumbing
  //   One database per namespace: DB_NAME_PREFIX + namespace
  //   One object store ("data") using out-of-line string keys.
  //   Stored values are Blobs (binary-safe, supports MIME round-tripping).
  // ---------------------------------------------------------------------------

  /** @type {Promise<IDBDatabase> | null} */
  let dbPromise = null;

  const openDB = () => {
    const namespace = getNamespace();
    if (!namespace) {
      return Promise.reject(new Error("IndexedDB Storage: no namespace set"));
    }
    if (dbPromise) {
      return dbPromise;
    }
    const dbName = `${DB_NAME_PREFIX}${namespace}`;
    dbPromise = new Promise((resolve, reject) => {
      let req;
      try {
        req = indexedDB.open(dbName, 1);
      } catch (err) {
        reject(err);
        return;
      }
      req.onupgradeneeded = (event) => {
        const db = /** @type {IDBDatabase} */ (event.target.result);
        if (!db.objectStoreNames.contains(STORE_NAME)) {
          db.createObjectStore(STORE_NAME);
        }
      };
      req.onsuccess = () => {
        const db = /** @type {IDBDatabase} */ (req.result);
        db.onversionchange = () => {
          // Another tab wants to upgrade; let it.
          try {
            db.close();
          } catch (err) {
            // ignore
          }
          dbPromise = null;
        };
        resolve(db);
      };
      req.onerror = () => reject(req.error);
      req.onblocked = () => reject(new Error("IndexedDB open blocked"));
    });
    // If the open fails, drop the cached promise so the next call can retry.
    dbPromise.catch(() => {
      dbPromise = null;
    });
    return dbPromise;
  };

  const closeDB = () => {
    if (dbPromise) {
      const pending = dbPromise;
      dbPromise = null;
      pending.then(
        (db) => {
          try {
            db.close();
          } catch (err) {
            // ignore
          }
        },
        () => {
          // open failed; nothing to close
        }
      );
    }
  };

  /**
   * Run a transaction and resolve when it commits.
   * @param {"readonly"|"readwrite"} mode
   * @param {(store: IDBObjectStore) => void} use
   */
  const txRun = (mode, use) =>
    openDB().then(
      (db) =>
        new Promise((resolve, reject) => {
          const tx = db.transaction(STORE_NAME, mode);
          const store = tx.objectStore(STORE_NAME);
          try {
            use(store);
          } catch (err) {
            reject(err);
            return;
          }
          tx.oncomplete = () => resolve();
          tx.onerror = () => reject(tx.error);
          tx.onabort = () => reject(tx.error || new Error("Transaction aborted"));
        })
    );

  /**
   * Get the stored Blob for a key.
   * @param {string} key
   * @returns {Promise<Blob | null>}
   */
  const idbGet = (key) =>
    openDB().then(
      (db) =>
        new Promise((resolve, reject) => {
          const tx = db.transaction(STORE_NAME, "readonly");
          const store = tx.objectStore(STORE_NAME);
          const req = store.get(key);
          req.onsuccess = () => {
            const result = req.result;
            resolve(result && result instanceof Blob ? result : null);
          };
          req.onerror = () => reject(req.error);
        })
    );

  const idbPut = (key, value) => txRun("readwrite", (store) => store.put(value, key));
  const idbDelete = (key) => txRun("readwrite", (store) => store.delete(key));
  const idbClear = () => txRun("readwrite", (store) => store.clear());

  // ---------------------------------------------------------------------------
  // BroadcastChannel plumbing
  //   BroadcastChannel fires the `message` event only on OTHER instances in
  //   the same channel (mirroring the `localStorage.storage` event the original
  //   Local Storage extension uses). That's exactly what we want for the
  //   "when another window changes storage" hat.
  // ---------------------------------------------------------------------------

  /** @type {BroadcastChannel | null} */
  let channel = null;

  const ensureChannel = () => {
    if (typeof BroadcastChannel === "undefined") return;
    const namespace = getNamespace();
    if (!namespace) return;
    const channelName = `${CHANNEL_NAME_PREFIX}${namespace}`;
    if (channel && channel.name === channelName) return;
    if (channel) {
      try {
        channel.close();
      } catch (err) {
        // ignore
      }
      channel = null;
    }
    channel = new BroadcastChannel(channelName);
    channel.onmessage = () => {
      // Only fire the hat while the namespace is still the one we opened for
      // (the user may have just switched to a different namespace).
      if (getNamespace() === namespace) {
        Scratch.vm.runtime.startHats(`${EXTENSION_ID}_whenChanged`);
      }
    };
  };

  const closeChannel = () => {
    if (channel) {
      try {
        channel.close();
      } catch (err) {
        // ignore
      }
      channel = null;
    }
  };

  const notifyOtherWindows = () => {
    if (channel) {
      try {
        channel.postMessage({ changed: true });
      } catch (err) {
        // ignore
      }
    }
  };

  // ---------------------------------------------------------------------------
  // Encoding utilities
  //   Input format comes from the FORMAT dropdown so we always know which
  //   decoder to use.
  // ---------------------------------------------------------------------------

  /**
   * Decode an input string to bytes + MIME.
   * @param {string} input
   * @param {"data URL"|"hex"|"base64"} format
   * @returns {{ bytes: Uint8Array, mime: string }}
   */
  const decodeToBytes = (input, format) => {
    input = Scratch.Cast.toString(input);
    if (format === "data URL") {
      // data:[<mime>][;base64],<data>
      // Allow newlines inside the payload (unlikely but safe).
      const match = /^data:([^;,]*)?(;base64)?,(.*)$/s.exec(input);
      if (!match) {
        // Not a valid data URL. Treat as empty bytes.
        return { bytes: new Uint8Array(0), mime: DEFAULT_MIME };
      }
      const mime = match[1] || DEFAULT_MIME;
      const isBase64 = !!match[2];
      const payload = match[3];
      let bytes;
      if (isBase64) {
        try {
          const binStr = atob(payload);
          bytes = new Uint8Array(binStr.length);
          for (let i = 0; i < binStr.length; i++) {
            bytes[i] = binStr.charCodeAt(i);
          }
        } catch (err) {
          bytes = new Uint8Array(0);
        }
      } else {
        // URL-encoded text data
        try {
          const decoded = decodeURIComponent(payload);
          bytes = new TextEncoder().encode(decoded);
        } catch (err) {
          // Fall back to raw text if decoding fails
          bytes = new TextEncoder().encode(payload);
        }
      }
      return { bytes, mime };
    }
    if (format === "hex") {
      // Strip whitespace & non-hex characters, then take pairs.
      const cleaned = input.replace(/[^0-9a-fA-F]/g, "");
      const safeLen = Math.floor(cleaned.length / 2) * 2;
      const bytes = new Uint8Array(safeLen / 2);
      for (let i = 0; i < bytes.length; i++) {
        bytes[i] = parseInt(cleaned.substr(i * 2, 2), 16);
      }
      return { bytes, mime: DEFAULT_MIME };
    }
    // base64
    const b64 = input.replace(/\s+/g, "");
    let bytes;
    try {
      const binStr = atob(b64);
      bytes = new Uint8Array(binStr.length);
      for (let i = 0; i < binStr.length; i++) {
        bytes[i] = binStr.charCodeAt(i);
      }
    } catch (err) {
      bytes = new Uint8Array(0);
    }
    return { bytes, mime: DEFAULT_MIME };
  };

  /**
   * Encode a Blob into the requested format.
   * @param {Blob} blob
   * @param {"data URL"|"hex"|"base64"} format
   * @returns {Promise<string>}
   */
  const encodeFromBlob = async (blob, format) => {
    if (format === "data URL") {
      return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(/** @type {string} */ (reader.result));
        reader.onerror = () => reject(reader.error);
        reader.readAsDataURL(blob);
      });
    }
    const buffer = await blob.arrayBuffer();
    const u8 = new Uint8Array(buffer);
    if (format === "hex") {
      let out = "";
      // Batch in chunks for performance on large arrays.
      const CHUNK = 0x10000;
      for (let i = 0; i < u8.length; i += CHUNK) {
        const end = Math.min(i + CHUNK, u8.length);
        let chunk = "";
        for (let j = i; j < end; j++) {
          chunk += u8[j].toString(16).padStart(2, "0");
        }
        out += chunk;
      }
      return out;
    }
    // base64
    let binStr = "";
    const CHUNK = 0x8000;
    for (let i = 0; i < u8.length; i += CHUNK) {
      const end = Math.min(i + CHUNK, u8.length);
      binStr += String.fromCharCode.apply(
        null,
        Array.from(u8.subarray(i, end))
      );
    }
    return btoa(binStr);
  };

  // ---------------------------------------------------------------------------
  // Lifecycle hooks
  // ---------------------------------------------------------------------------

  Scratch.vm.runtime.on("PROJECT_LOADED", () => {
    prepareInitialNamespace();
  });

  Scratch.vm.runtime.on("RUNTIME_DISPOSED", () => {
    closeDB();
    closeChannel();
    // A new project will set its own namespace later via PROJECT_LOADED.
    if (Scratch.vm.runtime.extensionStorage[EXTENSION_ID]) {
      Scratch.vm.runtime.extensionStorage[EXTENSION_ID] = undefined;
    }
  });

  prepareInitialNamespace();

  let lastNamespaceWarning = 0;
  const validNamespace = () => {
    const valid = !!getNamespace();
    if (!valid && Date.now() - lastNamespaceWarning > 3000) {
      alert(
        Scratch.translate(
          'IndexedDB Storage extension: project must run the "set namespace to [ID]" block before it can use other blocks'
        )
      );
      lastNamespaceWarning = Date.now();
    }
    return valid;
  };

  // ---------------------------------------------------------------------------
  // Extension class
  // ---------------------------------------------------------------------------

  class IndexedDBStorage {
    getInfo() {
      return {
        id: EXTENSION_ID,
        name: Scratch.translate("IndexedDB Storage"),
        color1: "#5B2D8C",
        color2: "#4A2470",
        color3: "#7B4FB0",
        docsURI: "https://extensions.turbowarp.org/",
        blocks: [
          {
            blockType: Scratch.BlockType.LABEL,
            text: getNamespace()
              ? Scratch.translate(
                  { default: "Namespace: {namespace}" },
                  { namespace: getNamespace() }
                )
              : Scratch.translate("No namespace set"),
          },
          {
            opcode: "get",
            blockType: Scratch.BlockType.REPORTER,
            text: Scratch.translate("get [KEY] from storage as [FORMAT]"),
            arguments: {
              KEY: {
                type: Scratch.ArgumentType.STRING,
                defaultValue: Scratch.translate("score"),
              },
              FORMAT: {
                type: Scratch.ArgumentType.STRING,
                menu: "FORMAT_MENU",
                defaultValue: "data URL",
              },
            },
          },
          {
            opcode: "set",
            blockType: Scratch.BlockType.COMMAND,
            text: Scratch.translate(
              "set [KEY] to [VALUE] in storage as [FORMAT]"
            ),
            arguments: {
              KEY: {
                type: Scratch.ArgumentType.STRING,
                defaultValue: Scratch.translate("score"),
              },
              VALUE: {
                type: Scratch.ArgumentType.STRING,
                defaultValue: "",
              },
              FORMAT: {
                type: Scratch.ArgumentType.STRING,
                menu: "FORMAT_MENU",
                defaultValue: "data URL",
              },
            },
          },
          {
            opcode: "remove",
            blockType: Scratch.BlockType.COMMAND,
            text: Scratch.translate("delete [KEY] from storage"),
            arguments: {
              KEY: {
                type: Scratch.ArgumentType.STRING,
                defaultValue: Scratch.translate("score"),
              },
            },
          },
          {
            opcode: "clear",
            blockType: Scratch.BlockType.COMMAND,
            text: Scratch.translate("clear storage"),
          },
          {
            opcode: "whenChanged",
            blockType: Scratch.BlockType.EVENT,
            text: Scratch.translate("when another window changes storage"),
            isEdgeActivated: false,
          },
          "---",
          {
            opcode: "getAtOffset",
            blockType: Scratch.BlockType.REPORTER,
            text: Scratch.translate(
              "get [KEY] from storage at offset [OFFSET] as [FORMAT]"
            ),
            arguments: {
              KEY: {
                type: Scratch.ArgumentType.STRING,
                defaultValue: Scratch.translate("score"),
              },
              OFFSET: {
                type: Scratch.ArgumentType.NUMBER,
                defaultValue: 0,
              },
              FORMAT: {
                type: Scratch.ArgumentType.STRING,
                menu: "FORMAT_MENU",
                defaultValue: "data URL",
              },
            },
          },
          {
            opcode: "getAtOffsetLength",
            blockType: Scratch.BlockType.REPORTER,
            text: Scratch.translate(
              "get [KEY] from storage at [OFFSET] length [LEN] as [FORMAT]"
            ),
            arguments: {
              KEY: {
                type: Scratch.ArgumentType.STRING,
                defaultValue: Scratch.translate("score"),
              },
              OFFSET: {
                type: Scratch.ArgumentType.NUMBER,
                defaultValue: 0,
              },
              LEN: {
                type: Scratch.ArgumentType.NUMBER,
                defaultValue: 1,
              },
              FORMAT: {
                type: Scratch.ArgumentType.STRING,
                menu: "FORMAT_MENU",
                defaultValue: "data URL",
              },
            },
          },
          {
            opcode: "getLength",
            blockType: Scratch.BlockType.REPORTER,
            text: Scratch.translate("get length of [KEY] in storage"),
            arguments: {
              KEY: {
                type: Scratch.ArgumentType.STRING,
                defaultValue: Scratch.translate("score"),
              },
            },
          },
          "---",
          {
            opcode: "setProjectId",
            blockType: Scratch.BlockType.COMMAND,
            text: Scratch.translate("set namespace to [ID]"),
            arguments: {
              ID: {
                type: Scratch.ArgumentType.STRING,
                defaultValue:
                  getNamespace() || Scratch.translate("project title"),
              },
            },
          },
        ],
        menus: {
          FORMAT_MENU: {
            acceptReporters: true,
            items: ["data URL", "hex", "base64"],
          },
        },
      };
    }

    async get({ KEY, FORMAT }) {
      if (!validNamespace()) return "";
      ensureChannel();
      const key = Scratch.Cast.toString(KEY);
      const format = Scratch.Cast.toString(FORMAT) || "data URL";
      try {
        const blob = await idbGet(key);
        if (!blob) return "";
        return await encodeFromBlob(blob, format);
      } catch (err) {
        console.error("IndexedDB Storage: get failed", err);
        return "";
      }
    }

    async set({ KEY, VALUE, FORMAT }) {
      if (!validNamespace()) return;
      ensureChannel();
      const key = Scratch.Cast.toString(KEY);
      const format = Scratch.Cast.toString(FORMAT) || "data URL";
      try {
        const { bytes, mime } = decodeToBytes(VALUE, format);
        const blob = new Blob([bytes], { type: mime });
        await idbPut(key, blob);
        notifyOtherWindows();
      } catch (err) {
        console.error("IndexedDB Storage: set failed", err);
      }
    }

    async remove({ KEY }) {
      if (!validNamespace()) return;
      ensureChannel();
      const key = Scratch.Cast.toString(KEY);
      try {
        await idbDelete(key);
        notifyOtherWindows();
      } catch (err) {
        console.error("IndexedDB Storage: remove failed", err);
      }
    }

    async clear() {
      if (!validNamespace()) return;
      ensureChannel();
      try {
        await idbClear();
        notifyOtherWindows();
      } catch (err) {
        console.error("IndexedDB Storage: clear failed", err);
      }
    }

    async getAtOffset({ KEY, OFFSET, FORMAT }) {
      if (!validNamespace()) return "";
      ensureChannel();
      const key = Scratch.Cast.toString(KEY);
      const format = Scratch.Cast.toString(FORMAT) || "data URL";
      const offset = Math.max(0, Math.floor(Scratch.Cast.toNumber(OFFSET)));
      try {
        const blob = await idbGet(key);
        if (!blob) return "";
        if (offset >= blob.size) return "";
        // Only load the single byte at `offset`, not everything after it.
        const sliced = blob.slice(offset, offset + 1);
        return await encodeFromBlob(sliced, format);
      } catch (err) {
        console.error("IndexedDB Storage: getAtOffset failed", err);
        return "";
      }
    }

    async getAtOffsetLength({ KEY, OFFSET, LEN, FORMAT }) {
      if (!validNamespace()) return "";
      ensureChannel();
      const key = Scratch.Cast.toString(KEY);
      const format = Scratch.Cast.toString(FORMAT) || "data URL";
      const offset = Math.max(0, Math.floor(Scratch.Cast.toNumber(OFFSET)));
      // Treat negative / non-finite lengths as 0; cap at remaining bytes.
      const lenRaw = Math.floor(Scratch.Cast.toNumber(LEN));
      const len = Number.isFinite(lenRaw) && lenRaw > 0 ? lenRaw : 0;
      try {
        const blob = await idbGet(key);
        if (!blob) return "";
        if (offset >= blob.size || len === 0) return "";
        // Blob.slice clamps `end` to blob.size, so we don't need to compute it.
        const sliced = blob.slice(offset, offset + len);
        return await encodeFromBlob(sliced, format);
      } catch (err) {
        console.error("IndexedDB Storage: getAtOffsetLength failed", err);
        return "";
      }
    }

    async getLength({ KEY }) {
      if (!validNamespace()) return 0;
      ensureChannel();
      const key = Scratch.Cast.toString(KEY);
      try {
        const blob = await idbGet(key);
        if (!blob) return 0;
        return blob.size;
      } catch (err) {
        console.error("IndexedDB Storage: getLength failed", err);
        return 0;
      }
    }

    setProjectId({ ID }) {
      setNamespace(Scratch.Cast.toString(ID));
    }
  }

  Scratch.extensions.register(new IndexedDBStorage());
})(Scratch);
