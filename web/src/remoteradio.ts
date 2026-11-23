/** Remote HackRF over WebSocket — streaming raw IQ samples to a browser, controlled by JSON messages. */

import type { Radio, RadioCaps, RadioEvents, RadioLimits } from './radio.js';

/**
 * What a remote HackRF will do. Capped at 4 Msps to stay within reasonable network bandwidth.
 * Raw signed-int8 IQ requires ~2 MB/s per Msps before WebSocket overhead.
 */
export const REMOTE_LIMITS: RadioLimits = {
  minHz: 1_000_000,
  maxHz: 6_000_000_000,
  rates: [[1_000_000, 4_000_000]],
  canTransmit: false,
};

/** The MAX2837's baseband filter bandwidths (same as local HackRF). */
export const BB_FILTER_BW: number[] = [1750000, 2500000, 3500000, 5000000, 5500000, 6000000, 7000000, 8000000, 9000000, 10000000,
  12000000, 14000000, 15000000, 20000000, 24000000, 28000000];

/** Remote capabilities — same as HackRF except TX is disabled. */
export const REMOTE_CAPS: RadioCaps = {
  stagedGain: true, tunerGain: false, amp: true, biasT: true, biasVolts: 3.3,
  tunerAgc: false, rtlAgc: false, ppm: false, directSampling: false, bbFilters: BB_FILTER_BW,
};

/**
 * Configuration message sent to the server.
 * All fields are required and must be valid for the hardware.
 */
export interface RemoteConfig {
  freqHz: number;
  rateHz: number;
  lna: number;
  vga: number;
  amp: boolean;
  biasTee: boolean;
  bbFilterHz: number;
}

/**
 * Lifecycle messages for controlling streaming.
 */
export interface StartMsg { type: 'start'; }
export interface StopMsg { type: 'stop'; }
export type LifecycleMsg = StartMsg | StopMsg;

/**
 * RemoteHackRF — a driver that talks to a remote HackRF over WebSocket.
 */
export class RemoteHackRF implements Radio {
  readonly kind = 'hackrf' as const;
  readonly limits = REMOTE_LIMITS;
  readonly caps = REMOTE_CAPS;
  readonly label = 'Remote HackRF (WebSocket)';

  private readonly wsUrl: string;
  targetFreqHz: number;
  targetRateHz: number;

  running = false;

  private gainState = { lna: 16, vga: 20, amp: false };
  private pendingConfig: RemoteConfig | null = null;
  private configTimer = 0;

  private ws: WebSocket | null = null;
  private connected = false;
  private events: RadioEvents | null = null;

  constructor(url: string) {
    this.wsUrl = url;
    this.targetFreqHz = 145_000_000;
    this.targetRateHz = 2_000_000;
  }

  async open(): Promise<void> {
    if (this.ws) throw new Error('RemoteHackRF already open');
    if (!this.events) throw new Error('RemoteHackRF: set events before open()');

    return new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(this.wsUrl);
      this.ws = ws;
      ws.binaryType = 'arraybuffer';

      ws.onopen = () => {
        this.connected = true;
        this.sendConfig();
        resolve();
      };
      ws.onclose = () => {
        this.connected = false;
        this.ws = null;
        if (this.running && this.events) this.events.onLog('Remote connection closed');
      };
      ws.onerror = (err) => {
        this.connected = false;
        this.ws = null;
        reject(new Error(`WebSocket error: ${err instanceof Event ? err.type : String(err)}`));
      };
      ws.onmessage = (event) => {
        if (event.data instanceof ArrayBuffer) this.handleIq(event.data);
        else if (typeof event.data === 'string') this.events?.onLog(`Remote: ${event.data}`);
      };
    });
  }

  async run(): Promise<void> {
    if (this.running) return;
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) throw new Error('WebSocket not connected');
    if (!this.events) throw new Error('RemoteHackRF: set events before run()');
    this.running = true;
    this.sendLifecycle({ type: 'start' });
  }

  stop(): void {
    if (!this.running) return;
    this.running = false;
    this.sendLifecycle({ type: 'stop' });
  }

  setGain(lnaDb: number, vgaDb: number, amp: boolean): void {
    this.gainState = { lna: Math.min(40, Math.max(0, Math.floor(lnaDb / 8) * 8)),
      vga: Math.min(62, Math.max(0, Math.floor(vgaDb / 2) * 2)), amp };
    this.sendConfig();
  }

  setEvents(events: RadioEvents): void { this.events = events; }

  sendConfig(): void {
    if (!this.connected || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    if (this.configTimer) { clearTimeout(this.configTimer); this.configTimer = 0; }
    if (this.pendingConfig) return;

    const bbHz = this.targetRateHz >= 2_000_000 ? 2_000_000 : (this.targetRateHz >= 1_500_000 ? 1_750_000 : Math.max(1_000_000, Math.floor(this.targetRateHz / 1_000_000) * 1_000_000));
    this.pendingConfig = { freqHz: this.targetFreqHz, rateHz: this.targetRateHz, lna: this.gainState.lna, vga: this.gainState.vga, amp: this.gainState.amp, biasTee: false, bbFilterHz: bbHz };

    this.configTimer = window.setTimeout(() => {
      if (this.pendingConfig && this.connected && this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(this.pendingConfig));
      this.pendingConfig = null; this.configTimer = 0;
    }, 50);
  }

  private sendLifecycle(msg: LifecycleMsg): void {
    if (this.connected && this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(msg));
  }

  private handleIq(buffer: ArrayBuffer): void {
    if (!this.running || !this.events) return;
    this.events.onIq(new Int8Array(buffer));
  }

  close(): void {
    this.stop();
    if (this.ws) { this.ws.close(); this.ws = null; }
    this.connected = false;
    if (this.configTimer) { clearTimeout(this.configTimer); this.configTimer = 0; }
  }
}
