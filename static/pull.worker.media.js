const separator = 0x77656274616C6Bn; // "webtalk" as a 64-bit BigInt
const boxLen = 19;
let pl = null;

class pipeline {
  constructor (reader) {
    this.stopped = false;
    this.reader = reader;

    this.buffer = new Uint8Array();
    this.decoderHandler = {
      'acnf': this.handleAudioConfig.bind(this),
      'vcnf': this.handleVideoConfig.bind(this),
      'afrm': this.handleAudioFrame.bind(this),
      'vfrm': this.handleVideoFrame.bind(this)
    };
    this.decoderMap = {};
  }
  MediaAVStream () {
    let ctx = this ;
    return new TransformStream ({
      async start (controller) {},
      transform (chunk, controller) {
        const newBuffer = new Uint8Array(ctx.buffer.length + chunk.byteLength);
        newBuffer.set(ctx.buffer);
        newBuffer.set(chunk, ctx.buffer.length);
        ctx.buffer = newBuffer;
        while (ctx.parseNextBox()) { }
        for (let i in ctx.decoderMap) {
          if (Date.now() - ctx.decoderMap[i].hb > 10000)  {
            ctx.stopOne(i);
          }
        }
      },
      async flush () {},
    });
  }

  parseNextBox() {
    // Need at least 9 bytes (separator + ssrc + type + length)
    if (this.buffer.length < boxLen) return false;
    // Find separator (0x00)
    const separatorIndex = this.findSeparatorIndexFast();
    if (separatorIndex < 0) {
      // No separator found, discard buffer
      return false;
    }
    // Skip to separator position
    if (separatorIndex > 0) {
      console.log(`Skipping ${separatorIndex} bytes to find separator`);
      this.buffer = this.buffer.slice(separatorIndex);
    }
    // Check if we have complete header
    if (this.buffer.length < boxLen) return false;
    const ssrcBytes = this.buffer.slice(7, 11);
    const ssrc = new DataView(ssrcBytes.buffer).getUint32(0, false);

    const typeBytes = this.buffer.slice(11, 15);
    const type = String.fromCharCode(...typeBytes);

    if (type != "acnf" &&type != "afrm" &&type != "vcnf" &&type != "vfrm") {
      this.buffer = this.buffer.slice(15);
      return true;
    }
    const lengthBytes = this.buffer.slice(15, 19);
    const payloadLength = new DataView(lengthBytes.buffer).getUint32(0, false);
    // Check if we have complete box
    const totalBoxLength = boxLen + payloadLength;
    if (this.buffer.length < totalBoxLength) return false;
    // Extract payload
    const payload = this.buffer.slice(boxLen, boxLen + payloadLength);

    // Process the box
    this.processBox(ssrc, type, payload);
    // Remove processed box from buffer
    this.buffer = this.buffer.slice(totalBoxLength);
    return true;
  }
  processBox(ssrc, type, payload) {
    if (!this.decoderMap.hasOwnProperty(ssrc)) {
      // let audioTrackGenerator = new VideoTrackGenerator();
      // let videoTrackGenerator = new AudioTrackGenerator();
      this.decoderMap[ssrc] = {};
    }
    let shouldHandle = false;
    let isVideo = type === "vcnf" || type === "vfrm";
    let isAudio = type === "acnf" || type === "afrm";
    if (!this.decoderMap[ssrc].videoDecoder && isVideo) {
      if (!this.decoderMap[ssrc].videoCache) {
        this.decoderMap[ssrc].videoCache = [];
        self.postMessage ({type:"new", ssrc, kind: type});
      }      
      if (this.decoderMap[ssrc].videoCache.length > 1024) {
        this.decoderMap[ssrc].videoCache.splice(0);
      }
      this.decoderMap[ssrc].videoCache.push({type, payload});
    } else {
      shouldHandle = true
    }
    if (!this.decoderMap[ssrc].audioDecoder && isAudio) {
      if (!this.decoderMap[ssrc].audioCache) {
        this.decoderMap[ssrc].audioCache = [];
        self.postMessage ({type:"new", ssrc, kind: type});
      }
      if (this.decoderMap[ssrc].audioCache.length > 1024) {
        this.decoderMap[ssrc].audioCache.splice(0);
      }
      this.decoderMap[ssrc].audioCache.push({type, payload});
    } else {
      shouldHandle = true
    }
    if (!shouldHandle) {
      return;
    }
    const handler = this.decoderHandler[type];
    if (handler) {
      handler(ssrc, payload);
    } else {
      this.debugLog(`Unknown box type: ${type}`);
    }
  }

  // Handle audio configuration
  async handleAudioConfig(ssrc, payload) {
    console.log("handleAudioConfig", ssrc)
    let map = this.decoderMap[ssrc];
    if (!map || !map.audioDecoder) {
      return;
    }
    const view = new DataView(payload.buffer);
    let cursor = 0;
    let sampleRate = view.getUint16(cursor, false);
    cursor += 2;
    let numberOfChannels = view.getUint8(cursor, false);
    cursor += 1;
    let codec = payload.slice(cursor);
    const codecString = new TextDecoder().decode(codec);
    let audioConfig = {
      codec: codecString,
      sampleRate,
      numberOfChannels
    };
    console.log(`Audio ${ssrc} config:`, audioConfig);
    map.audioConfig = audioConfig;
    map.audioDecoder.configure(audioConfig);
    console.log("handleAudioConfig done", ssrc)
  }

  // Handle video configuration
  async handleVideoConfig(ssrc, payload) {
    console.log("handleVideoConfig", ssrc)
    let map = this.decoderMap[ssrc];
    if (!map || !map.videoDecoder) {
      return;
    }
    const view = new DataView(payload.buffer);
    let cursor = 0;
    let width = view.getUint16(cursor);
    cursor += 2;
    let height = view.getUint16(cursor);
    cursor += 2;
    const codecString = new TextDecoder().decode(payload.subarray(cursor));
    let videoConfig = {
      codec: codecString,
      codedWidth: width,
      codedHeight: height,
      avc: { format: "annexb" }
    };
    console.log(`Video ${ssrc} config: `, videoConfig);
    map.videoConfig = videoConfig;
    map.state = -1;
    map.videoDecoder.configure(videoConfig);
    console.log("handleVideoConfig done", ssrc)
  }

  // Handle audio frame
  handleAudioFrame(ssrc, payload) {
    // console.log("handleAudioFrame", ssrc)
    let map = this.decoderMap[ssrc];
    if (!map || !map.audioDecoder || map.audioDecoder.state !== 'configured') {
      this.debugLog('Audio decoder not ready');
      return;
    }
    const chunk = new EncodedAudioChunk({
      type: "key",
      timestamp: -1,
      duration: 0,
      data: payload
    });
    map.audioDecoder.decode(chunk);
    // console.log("handleAudioFrame done", ssrc)
  }

  // Handle video frame
  handleVideoFrame(ssrc, payload) {
    // console.log("handleVideoFrame", ssrc)
    let map = this.decoderMap[ssrc];
    if (!map || !map.videoDecoder || map.videoDecoder.state !== 'configured') {
      this.debugLog('Video decoder not ready');
      return;
    }
    let isKeyframe = payload[0] === 1;
    let data = payload.slice(1);
    if (map.state === -1 && !isKeyframe) {
      return;
    }
    const chunk = new EncodedVideoChunk({
      type: isKeyframe ? 'key' : 'delta',
      // timestamp: Number(timestamp),
      timestamp: -1,
      data
    });
    map.videoDecoder.decode(chunk);
    if (map.state === -1) {
      map.state = 0;
    }
    // console.log("handleVideoFrame done", ssrc)
    // this.videoTimestamp += BigInt(timestamp * 1000 / 90000);
    // let duration = Date.now() - start_time;
  }

  findSeparatorIndexFast(startIndex = 0) {
    if (this.buffer.byteLength < 8) {
      return -2;
    }
    const view = new DataView(this.buffer.buffer);
    for (let i = startIndex; i <= this.buffer.byteLength - 8; i++) {
      // Read 8 bytes (we only need 7, but read 8 for comparison)
      const word = view.getBigUint64(i, false); // Big-endian

      // Shift right by 8 bits to compare 7 bytes
      if ((word >> 8n) === separator) {
        return i;
      }
    }
    return -1;
  }
  register({ssrc, audioWritable, videoWritable}) {
    let me = this;
    if (audioWritable) {
      let audioWriter = audioWritable.getWriter();
      let audioDecoder = new AudioDecoder({
        output: (frame) => {
          if (me.decoderMap[ssrc]) {
            let now = Date.now();
            if (me.decoderMap[ssrc].hb && now - me.decoderMap[ssrc].hb > 1000) {
              me.decoderMap[ssrc].hb = Date.now();
            }
          }
          audioWriter.write(frame)
        },
        error: console.error
      });
      this.decoderMap[ssrc].audioDecoder = audioDecoder;
      this.decoderMap[ssrc].hb = Date.now();
      let cache = this.decoderMap[ssrc].audioCache;
      while (cache.length > 0) {
        let {type, payload} = cache.splice(0,1)[0];
        const handler = this.decoderHandler[type];
        if (handler) {
          handler(ssrc, payload);
        } else {
          this.debugLog(`Unknown box type: ${type}`);
        }
      }
      delete this.decoderMap[ssrc].audioCache;
    }
    if (videoWritable) {
      console.log("registering video")
      let videoWriter = videoWritable.getWriter();
      let videoDecoder = new VideoDecoder({
        output: (frame) => {
          let now = Date.now();
          if (me.decoderMap[ssrc].hb && now - me.decoderMap[ssrc].hb > 1000) {
            me.decoderMap[ssrc].hb = Date.now();
          }
          videoWriter.write(frame)
        },
        error: console.error
      });
      this.decoderMap[ssrc].videoDecoder = videoDecoder;
      this.decoderMap[ssrc].hb = Date.now();
      let cache = this.decoderMap[ssrc].videoCache;
      while (cache.length > 0) {
        let {type, payload} = cache.splice(0,1)[0];
        const handler = this.decoderHandler[type];
        if (handler) {
          handler(ssrc, payload);
        } else {
          this.debugLog(`Unknown box type: ${type}`);
        }
      }
      delete this.decoderMap[ssrc].videoCache;
    }
  }
  stop (ssrc) {
    // if (encoder.state != 'closed') encoder.close ();
    // stopped = true;
    // this.stopped = true;
    if (ssrc) {
      this.stopOne(ssrc);
    } else {
      for (let i in this.decoderMap) {
        this.stopOne(i);
      }
    }
    return;
  }

  stopOne (ssrc) {
    // console.log("stopping ", ssrc)
    let map = this.decoderMap[ssrc];
    if (!map) {
      return;
    }
    if (map.videoDecoder && map.videoDecoder.state != 'closed') map.videoDecoder.close ();
    if (map.videoDecoder && map.audioDecoder.state != 'closed') map.audioDecoder.close ();
    map.state = -1;
    delete this.decoderMap[ssrc];
    self.postMessage ({type: 'del', ssrc});
  }
  async start () {
    // if (stopped) return;
    // started = true;
    try {
      await this.reader
        .pipeThrough (this.MediaAVStream())
        .pipeTo (
          new WritableStream ({
            write (chunk) {
              self.postMessage ({
                type: 'data',
                data: chunk,
              });
            },
          })
        );
    } catch (e) {
      self.postMessage ({severity: 'fatal', text: `start error: ${e.message}`});
    }
  }

  debugLog(msg) {
    console.log("debug",msg);
  }
}

onmessage = async (data)=>{
  let {ssrc, reader, audioWritable, videoWritable, type} = data.data;
  if (type === "start") {
    if (pl) {
      pl.stop();
    }
    try {
      pl = new pipeline (reader);
      pl.start ();
    } catch (e) {
      self.postMessage ({
        severity: 'fatal',
        text: `Pipeline creation failed: ${e.message}`,
      });
      return;
    }
    // self.streamFetcher = new StreamFetcher(url, audioWritable, videoWritable);  
    // await self.streamFetcher.start();
  } else if (type === "register") {
    if (pl) {
      pl.register({ssrc, audioWritable, videoWritable});
    }
  } else if (type === "stop") {
    if (pl) {
      pl.stop(ssrc);
    }
  }
}