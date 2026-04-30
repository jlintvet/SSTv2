import React, { useState, useEffect, useMemo, useRef, useCallback } from "react";
import { base44 } from "@/api/base44Client";
import { Maximize2, Minimize2, Crosshair, Move, Play, Pause, Wind } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import MapClickInfo from "@/components/MapClickInfo";
import SavedLocations from "@/components/SavedLocations";
import SSTLegend from "@/components/SSTLegend";

import L from "leaflet";
import "leaflet/dist/leaflet.css";

// Inject leaflet-velocity from CDN (loaded once)
if (typeof document !== "undefined" && !document.getElementById("leaflet-velocity-script")) {
  const link = document.createElement("link");
  link.rel = "stylesheet";
  link.href = "https://cdn.jsdelivr.net/npm/leaflet-velocity@1.9.2/dist/leaflet-velocity.css";
  document.head.appendChild(link);

  const script = document.createElement("script");
  script.id = "leaflet-velocity-script";
  script.src = "https://cdn.jsdelivr.net/npm/leaflet-velocity@1.9.2/dist/leaflet-velocity.min.js";
  document.head.appendChild(script);
}

// Override Tailwind preflight
if (typeof document !== "undefined" && !document.getElementById("leaflet-tw-fix")) {
  const s = document.createElement("style");
  s.id = "leaflet-tw-fix";
  s.textContent = `
    .leaflet-container img.leaflet-image-layer,
    .leaflet-container img.leaflet-tile,
    .leaflet-pane img { max-width: none !important; max-height: none !important; }
    .leaflet-velocity-layer { pointer-events: none; }
  `;
  document.head.appendChild(s);
}

import AuthGate from "@/components/auth/AuthGate";
import TrialBanner from "@/components/auth/TrialBanner";
import UserMenu from "@/components/auth/UserMenu";
import RegionGate from "@/components/region/RegionGate";
import { useRegionAccess } from "@/hooks/useRegionAccess";
import { getRegionConfig, DEFAULT_REGION } from "@/config/regionConfig";

// ── Color helpers ─────────────────────────────────────────────────────────────
function interpColor(t, stops) {
  let lower = stops[0], upper = stops[stops.length - 1];
  for (let i = 0; i < stops.length - 1; i++) {
    if (t >= stops[i][0] && t <= stops[i + 1][0]) { lower = stops[i]; upper = stops[i + 1]; break; }
  }
  const lt = (t - lower[0]) / (upper[0] - lower[0]);
  return [
    Math.round(lower[1][0] + (upper[1][0] - lower[1][0]) * lt),
    Math.round(lower[1][1] + (upper[1][1] - lower[1][1]) * lt),
    Math.round(lower[1][2] + (upper[1][2] - lower[1][2]) * lt),
  ];
}
const SST_STOPS = [[0,[15,40,140]],[0.2,[0,130,200]],[0.4,[0,200,180]],[0.6,[50,210,50]],[0.75,[255,220,0]],[0.9,[255,120,0]],[1,[220,30,30]]];
const CHL_STOPS = [[0,[10,40,130]],[0.25,[0,100,180]],[0.5,[0,170,100]],[0.75,[120,200,0]],[1,[200,160,0]]];
const KD_STOPS  = [[0,[10,60,160]],[0.3,[0,140,170]],[0.6,[0,160,80]],[0.85,[100,150,20]],[1,[150,100,0]]];

// Wind speed color scale (knots): calm blue → green → yellow → orange → red
const WIND_COLOR_SCALE = [
  "#0000ff","#0055ff","#0099ff","#00ccff",
  "#00ffcc","#00ff88","#00ff00","#88ff00",
  "#ccff00","#ffff00","#ffcc00","#ff9900",
  "#ff6600","#ff3300","#ff0000","#cc0000",
];

function sstColor(val, min, max) {
  if (val == null || !Number.isFinite(val)) return null;
  return interpColor(Math.max(0, Math.min(1, (val-min)/(max-min))), SST_STOPS);
}
function chlColor(val, min, max) {
  if (val == null || !Number.isFinite(val)) return null;
  const lMin=Math.log10(Math.max(min,0.001)), lMax=Math.log10(Math.max(max,0.01));
  return interpColor(Math.max(0, Math.min(1, (Math.log10(val)-lMin)/(lMax-lMin))), CHL_STOPS);
}
function kd490Color(val, min, max) {
  if (val == null || !Number.isFinite(val)) return null;
  return interpColor(Math.max(0, Math.min(1, (val-min)/(max-min))), KD_STOPS);
}

// Wind speed → color using WIND_COLOR_SCALE stops (defined later, but same palette)
// Mirrors what Windy shows: smooth gradient fill behind direction particles
const WIND_SPEED_STOPS = [
  [0,    [0,   0,   255]],
  [0.07, [0,   85,  255]],
  [0.14, [0,   153, 255]],
  [0.21, [0,   204, 255]],
  [0.28, [0,   255, 204]],
  [0.35, [0,   255, 136]],
  [0.43, [0,   255, 0  ]],
  [0.5,  [136, 255, 0  ]],
  [0.57, [204, 255, 0  ]],
  [0.64, [255, 255, 0  ]],
  [0.71, [255, 204, 0  ]],
  [0.78, [255, 153, 0  ]],
  [0.85, [255, 102, 0  ]],
  [0.92, [255, 51,  0  ]],
  [1.0,  [255, 0,   0  ]],
];
function windSpeedColor(val, min, max) {
  if (val == null || !Number.isFinite(val) || val < 0) return null;
  return interpColor(Math.max(0, Math.min(1, (val - min) / (max - min))), WIND_SPEED_STOPS);
}

// ── Constants ─────────────────────────────────────────────────────────────────
const BATHY_URL          = "https://raw.githubusercontent.com/jlintvet/SSTv2/main/DailySST/bathymetry.json";
const BATHY_CONTOURS_URL = "https://raw.githubusercontent.com/jlintvet/SSTv2/main/DailySST/bathymetry_contours.json";
const WRECKS_URL         = "https://raw.githubusercontent.com/jlintvet/SSTv2/main/DailySST/wrecks.json";

function distanceNm(lat1,lon1,lat2,lon2){const R=3440.065,dLat=((lat2-lat1)*Math.PI)/180,dLon=((lon2-lon1)*Math.PI)/180;const a=Math.sin(dLat/2)**2+Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLon/2)**2;return R*2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));}
function bearingDeg(lat1,lon1,lat2,lon2){const dLon=((lon2-lon1)*Math.PI)/180;const y=Math.sin(dLon)*Math.cos(lat2*Math.PI/180);const x=Math.cos(lat1*Math.PI/180)*Math.sin(lat2*Math.PI/180)-Math.sin(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.cos(dLon);return((Math.atan2(y,x)*180/Math.PI)+360)%360;}
function bearingLabel(deg){return["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"][Math.round(deg/22.5)%16];}

// ── Wind time slider utils ────────────────────────────────────────────────────
function formatWindTime(isoStr) {
  if (!isoStr) return "";
  const d = new Date(isoStr + "Z");
  const days = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
  const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  return `${days[d.getUTCDay()]} ${months[d.getUTCMonth()]} ${d.getUTCDate()} · ${String(d.getUTCHours()).padStart(2,"0")}:00z`;
}
function formatWindTimeShort(isoStr) {
  if (!isoStr) return "";
  const d = new Date(isoStr + "Z");
  return `${String(d.getUTCHours()).padStart(2,"0")}z`;
}
function isDayBoundary(isoStr) {
  if (!isoStr) return false;
  return new Date(isoStr + "Z").getUTCHours() === 0;
}

// ── Wind speed legend — Windy style ──────────────────────────────────────────
// Color bar with evenly-spaced tick labels underneath, unit label on left
function WindLegend({ maxSpeed }) {
  const BAR_W = 200;  // px width of the color gradient bar
  // Tick values: choose sensible stops up to maxSpeed
  const allTicks = [0, 5, 10, 15, 20, 25, 30, 35, 40, 50, 60];
  const ticks = allTicks.filter(t => t <= Math.ceil(maxSpeed / 5) * 5 + 5);
  const scaleMax = ticks[ticks.length - 1];

  return (
    <div className="flex items-center gap-2">
      {/* Unit label */}
      <span className="text-[11px] text-white/70 font-medium flex-shrink-0">kt</span>
      {/* Bar + ticks block */}
      <div className="flex-shrink-0" style={{ width: BAR_W }}>
        {/* Gradient bar */}
        <div
          className="rounded-sm"
          style={{
            height: 10,
            background: `linear-gradient(to right, ${WIND_COLOR_SCALE.join(",")})`,
          }}
        />
        {/* Tick labels positioned relative to bar width */}
        <div className="relative" style={{ height: 14 }}>
          {ticks.map(t => (
            <span
              key={t}
              className="absolute text-[9px] text-white/80 tabular-nums font-medium"
              style={{
                left: `${(t / scaleMax) * 100}%`,
                transform: "translateX(-50%)",
                top: 1,
              }}
            >
              {t}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}

// ── Wind time slider — Windy style ───────────────────────────────────────────
// Dark translucent bar at bottom. Day columns with labels. Tooltip bubble above
// thumb showing current time. Play/pause on the left. Legend on the right.
function WindTimeSlider({ windData, windHourIndex, setWindHourIndex, isPlaying, setIsPlaying, isWindMap }) {
  const hours  = windData?.hours ?? [];
  const nHours = hours.length;
  const playRef = useRef(null);
  const trackRef = useRef(null);

  useEffect(() => {
    if (isPlaying) {
      playRef.current = setInterval(() => {
        setWindHourIndex(i => {
          if (i >= nHours - 1) { setIsPlaying(false); return i; }
          return i + 1;
        });
      }, 350);
    } else {
      clearInterval(playRef.current);
    }
    return () => clearInterval(playRef.current);
  }, [isPlaying, nHours]);

  if (!nHours) return null;

  const currentTime = hours[windHourIndex]?.time ?? "";
  const maxSpeed    = windData?.maxSpeed ?? 30;

  // Group hours into days for column rendering
  const days = [];
  let curDay = null;
  hours.forEach((h, i) => {
    const d   = new Date(h.time + "Z");
    const key = `${d.getUTCFullYear()}-${d.getUTCMonth()}-${d.getUTCDate()}`;
    const DAY_NAMES = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
    if (!curDay || curDay.key !== key) {
      curDay = { key, label: `${DAY_NAMES[d.getUTCDay()]} ${d.getUTCDate()}`, startIdx: i, count: 0 };
      days.push(curDay);
    }
    curDay.count++;
  });

  // Thumb position as % for tooltip bubble
  const thumbPct = nHours > 1 ? (windHourIndex / (nHours - 1)) * 100 : 0;

  // Format tooltip: "Thu 30 - 2 PM" style (like Windy)
  function fmtTooltip(isoStr) {
    if (!isoStr) return "";
    const d = new Date(isoStr + "Z");
    const DAY_NAMES = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
    const h = d.getUTCHours();
    const ampm = h === 0 ? "12 AM" : h < 12 ? `${h} AM` : h === 12 ? "12 PM" : `${h-12} PM`;
    return `${DAY_NAMES[d.getUTCDay()]} ${d.getUTCDate()} - ${ampm}`;
  }

  return (
    <div
      className="absolute bottom-0 left-0 right-0 z-[1200] select-none"
      style={{ background: "rgba(23,28,38,0.72)", backdropFilter: "blur(8px)" }}
    >
      {/* Tooltip bubble above thumb */}
      <div className="relative" style={{ height: 28, pointerEvents: "none" }}>
        <div
          className="absolute flex flex-col items-center"
          style={{ left: `calc(52px + (100% - 52px - 8px) * ${thumbPct/100})`, transform: "translateX(-50%)", top: 4 }}
        >
          <div
            className="text-[11px] font-semibold text-white px-2 py-0.5 rounded"
            style={{ background: "#f59e0b", whiteSpace: "nowrap", boxShadow: "0 1px 4px rgba(0,0,0,0.4)" }}
          >
            {fmtTooltip(currentTime)}
          </div>
        </div>
      </div>

      {/* Main bar: play button | day columns track | legend */}
      <div className="flex items-stretch" style={{ height: 52 }}>

        {/* Play button */}
        <div className="flex-shrink-0 flex items-center justify-center px-3" style={{ width: 52 }}>
          <button
            onClick={() => { if (windHourIndex >= nHours - 1) setWindHourIndex(0); setIsPlaying(p => !p); }}
            className="w-9 h-9 rounded-full flex items-center justify-center transition-colors"
            style={{ background: isPlaying ? "#374151" : "#374151", border: "2px solid #6b7280" }}
          >
            {isPlaying
              ? <Pause className="w-4 h-4 text-white" />
              : <Play  className="w-4 h-4 text-white ml-0.5" />}
          </button>
        </div>

        {/* Day columns + slider track */}
        <div className="flex-1 relative flex flex-col justify-end pb-1 pr-1" ref={trackRef}>
          {/* Day column headers — click to jump, bold when active */}
          <div className="flex absolute top-0 left-0 right-0" style={{ height: 22 }}>
            {days.map((day, di) => {
              const isActive = windHourIndex >= day.startIdx && windHourIndex < day.startIdx + day.count;
              return (
                <div
                  key={day.key}
                  onClick={() => { setIsPlaying(false); setWindHourIndex(day.startIdx); }}
                  className="flex items-center justify-center border-r border-white/10 text-[11px] cursor-pointer select-none transition-colors hover:bg-white/10"
                  style={{
                    width: `${(day.count / nHours) * 100}%`,
                    color: isActive ? "#fff" : "#9ca3af",
                    fontWeight: isActive ? 700 : 400,
                    background: di % 2 === 0 ? "rgba(255,255,255,0.03)" : "transparent",
                  }}
                >
                  {day.label}
                </div>
              );
            })}
          </div>

          {/* Range slider */}
          <input
            type="range"
            min={0}
            max={nHours - 1}
            value={windHourIndex}
            onChange={e => { setIsPlaying(false); setWindHourIndex(Number(e.target.value)); }}
            className="w-full appearance-none cursor-pointer"
            style={{
              height: 24,
              accentColor: "#f59e0b",
              background: "transparent",
            }}
          />
        </div>

        {/* Legend on right — only in Wind Map mode, not overlay */}
        {isWindMap && (
          <div className="flex-shrink-0 flex items-center pr-4 pl-2">
            <WindLegend maxSpeed={maxSpeed} />
          </div>
        )}

      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// ISOTHERM CONTOUR ENGINE
// ─────────────────────────────────────────────────────────────────────────────
function buildField(latSet, lonSet, grid) {
  const rows = latSet.length, cols = lonSet.length;
  const field = new Float32Array(rows * cols).fill(NaN);
  for (let r = 0; r < rows; r++)
    for (let c = 0; c < cols; c++) {
      const v = grid[`${latSet[r]}_${lonSet[c]}`];
      if (v != null && Number.isFinite(v)) field[r * cols + c] = v;
    }
  return { field, rows, cols };
}
function lerp(v0, v1, iso) { if (Math.abs(v1 - v0) < 1e-9) return 0.5; return (iso - v0) / (v1 - v0); }
function marchingSquares(latSet, lonSet, field, rows, cols, isoValue) {
  const segments = [];
  const get = (r, c) => { if (r<0||r>=rows||c<0||c>=cols) return NaN; return field[r*cols+c]; };
  function edgePt(r, c, dir) {
    let lat, lon;
    if (dir===0){const t=lerp(get(r,c),get(r,c+1),isoValue);lat=latSet[r];lon=lonSet[c]+t*(lonSet[c+1]-lonSet[c]);}
    else if(dir===1){const t=lerp(get(r,c+1),get(r+1,c+1),isoValue);lon=lonSet[c+1];lat=latSet[r]+t*(latSet[r+1]-latSet[r]);}
    else if(dir===2){const t=lerp(get(r+1,c+1),get(r+1,c),isoValue);lat=latSet[r+1];lon=lonSet[c+1]+t*(lonSet[c]-lonSet[c+1]);}
    else{const t=lerp(get(r+1,c),get(r,c),isoValue);lon=lonSet[c];lat=latSet[r+1]+t*(latSet[r]-latSet[r+1]);}
    return [lon, lat];
  }
  const edgePairs={1:[[2,3]],2:[[1,2]],3:[[1,3]],4:[[0,1]],5:[[0,3],[1,2]],6:[[0,2]],7:[[0,3]],8:[[0,3]],9:[[0,2]],10:[[0,1],[2,3]],11:[[0,1]],12:[[1,3]],13:[[1,2]],14:[[2,3]]};
  for (let r=0;r<rows-1;r++) for (let c=0;c<cols-1;c++) {
    const v00=get(r,c),v01=get(r,c+1),v10=get(r+1,c),v11=get(r+1,c+1);
    if(!Number.isFinite(v00)||!Number.isFinite(v01)||!Number.isFinite(v10)||!Number.isFinite(v11))continue;
    const idx=(v00>=isoValue?8:0)|(v01>=isoValue?4:0)|(v11>=isoValue?2:0)|(v10>=isoValue?1:0);
    const pairs=edgePairs[idx];if(!pairs)continue;
    for(const[eA,eB]of pairs)segments.push([edgePt(r,c,eA),edgePt(r,c,eB)]);
  }
  if(!segments.length)return[];
  const Q=5,fmt=([lon,lat])=>`${lon.toFixed(Q)},${lat.toFixed(Q)}`;
  const startMap=new Map(),endMap=new Map();
  for(let i=0;i<segments.length;i++){const sk=fmt(segments[i][0]),ek=fmt(segments[i][1]);if(!startMap.has(sk))startMap.set(sk,[]);if(!endMap.has(ek))endMap.set(ek,[]);startMap.get(sk).push(i);endMap.get(ek).push(i);}
  const used=new Uint8Array(segments.length),lines=[];
  for(let i=0;i<segments.length;i++){
    if(used[i])continue;used[i]=1;const coords=[...segments[i]];
    let tail=fmt(coords[coords.length-1]),found=true;
    while(found){found=false;for(const j of(startMap.get(tail)||[])){if(!used[j]){used[j]=1;coords.push(segments[j][1]);tail=fmt(coords[coords.length-1]);found=true;break;}}if(!found)for(const j of(endMap.get(tail)||[])){if(!used[j]){used[j]=1;coords.push(segments[j][0]);tail=fmt(coords[coords.length-1]);found=true;break;}}}
    let head=fmt(coords[0]);found=true;
    while(found){found=false;for(const j of(endMap.get(head)||[])){if(!used[j]){used[j]=1;coords.unshift(segments[j][0]);head=fmt(coords[0]);found=true;break;}}if(!found)for(const j of(startMap.get(head)||[])){if(!used[j]){used[j]=1;coords.unshift(segments[j][1]);head=fmt(coords[0]);found=true;break;}}}
    if(coords.length>=2)lines.push(coords);
  }
  return lines;
}
function computeTempBreakContour(latSet,lonSet,field,rows,cols,targetTemp,sensitivity){
  const gradient=new Float32Array(rows*cols).fill(0);
  const get=(r,c)=>{if(r<0||r>=rows||c<0||c>=cols)return NaN;return field[r*cols+c];};
  for(let r=0;r<rows;r++)for(let c=0;c<cols;c++){const v=get(r,c);if(!Number.isFinite(v))continue;let maxDiff=0;for(const[dr,dc]of[[0,1],[0,-1],[1,0],[-1,0]]){const n=get(r+dr,c+dc);if(Number.isFinite(n))maxDiff=Math.max(maxDiff,Math.abs(v-n));}gradient[r*cols+c]=maxDiff;}
  const maskedField=new Float32Array(field);
  for(let i=0;i<maskedField.length;i++){if(gradient[i]<sensitivity)maskedField[i]=NaN;}
  return marchingSquares(latSet,lonSet,maskedField,rows,cols,targetTemp);
}
function buildIsothermLines(latSet,lonSet,grid,targetTemp,sensitivity){
  if(!latSet.length||!lonSet.length)return{isotherms:[],breaks:[]};
  const{field,rows,cols}=buildField(latSet,lonSet,grid);
  const iso=marchingSquares(latSet,lonSet,field,rows,cols,targetTemp).map(line=>line.map(([lon,lat])=>[lat,lon]));
  const brk=computeTempBreakContour(latSet,lonSet,field,rows,cols,targetTemp,sensitivity).map(line=>line.map(([lon,lat])=>[lat,lon]));
  return{isotherms:iso,breaks:brk};
}

function IsothermControls({enabled,onToggle,targetTemp,onTargetTemp,sensitivity,onSensitivity,sstMin,sstMax}){
  const clampedTarget=Math.max(sstMin,Math.min(sstMax,targetTemp));
  return(
    <div className="border-t border-slate-200 mt-0.5 pt-1.5">
      <button onClick={onToggle} className={`w-full flex items-center gap-1.5 text-[11px] font-semibold px-2 py-1.5 rounded-lg text-left transition-colors ${enabled?"bg-sky-500 text-white":"bg-white text-slate-600 hover:bg-slate-50 border border-slate-300"}`}>
        <span className="text-sm">~</span> Temp Break
      </button>
      {enabled&&(
        <div className="mt-1.5 space-y-2 px-1">
          <div>
            <div className="flex justify-between items-center mb-0.5"><span className="text-[10px] text-slate-500 font-medium">Target Temp</span><span className="text-[11px] font-bold text-sky-600 tabular-nums">{clampedTarget.toFixed(1)}F</span></div>
            <input type="range" min={Math.floor(sstMin)} max={Math.ceil(sstMax)} step={0.5} value={clampedTarget} onChange={e=>onTargetTemp(parseFloat(e.target.value))} className="w-full h-1.5 rounded-full appearance-none cursor-pointer accent-sky-500"/>
            <div className="flex justify-between text-[9px] text-slate-400 mt-0.5"><span>{Math.floor(sstMin)}</span><span>{Math.ceil(sstMax)}</span></div>
          </div>
          <div>
            <div className="flex justify-between items-center mb-0.5"><span className="text-[10px] text-slate-500 font-medium">Sensitivity</span><span className="text-[11px] font-bold text-violet-600 tabular-nums">+/-{sensitivity.toFixed(1)}F</span></div>
            <input type="range" min={0.5} max={8} step={0.5} value={sensitivity} onChange={e=>onSensitivity(parseFloat(e.target.value))} className="w-full h-1.5 rounded-full appearance-none cursor-pointer accent-violet-500"/>
            <div className="flex justify-between text-[9px] text-slate-400 mt-0.5"><span>0.5 fine</span><span>8 coarse</span></div>
          </div>
        </div>
      )}
    </div>
  );
}

// ── DEFENSIVE NORMALIZER ──────────────────────────────────────────────────────
function normalizeSSTResponse(res,sourceName,valueKey="sst"){
  const data=res?.data??res;const topKeys=data&&typeof data==="object"?Object.keys(data):[];const firstDay=data?.days?.[0];const firstGrid=firstDay?.grid;const isFC=data?.type==="FeatureCollection"&&Array.isArray(data?.features);
  console.log(`[SST:${sourceName}] response shape:`,{topLevelKeys:topKeys,hasDays:Array.isArray(data?.days),dayCount:data?.days?.length,firstGridLen:Array.isArray(firstGrid)?firstGrid.length:null,firstGridSample:Array.isArray(firstGrid)?firstGrid[0]:null,isFeatureCollection:isFC,featureCount:isFC?data.features.length:null});
  if(Array.isArray(data?.days)&&data.days.length>0&&Array.isArray(firstGrid)&&firstGrid.length>0){checkGridRegularity(firstGrid,sourceName);return data;}
  if(isFC){console.warn(`[SST:${sourceName}] WARNING: Received GeoJSON FeatureCollection -- normalizing.`);const grid=data.features.map(f=>{const c=f?.geometry?.coordinates;const v=f?.properties?.[valueKey];if(!Array.isArray(c)||c.length<2)return null;if(v==null||!Number.isFinite(v))return null;return{lon:c[0],lat:c[1],[valueKey]:v};}).filter(Boolean);if(!grid.length){console.error(`[SST:${sourceName}] FeatureCollection had no usable points.`);return{days:[]};}const vals=grid.map(d=>d[valueKey]);const stats={min:Math.min(...vals),max:Math.max(...vals)};const date=data.date??firstDay?.date??new Date().toISOString().slice(0,10);checkGridRegularity(grid,sourceName);return{days:[{date,grid,stats}]};}
  console.error(`[SST:${sourceName}] ERROR: Response shape not recognized.`,topKeys);return{days:[]};
}
function checkGridRegularity(grid,sourceName){if(!Array.isArray(grid)||grid.length<10)return;const uniqLats=new Set(),uniqLons=new Set();for(const p of grid){uniqLats.add(p.lat);uniqLons.add(p.lon);}const N=grid.length,ratio=(uniqLats.size*uniqLons.size)/N;if(ratio>10)console.warn(`[SST:${sourceName}] WARNING: Grid appears SCATTERED (ratio ${ratio.toFixed(1)}).`);else console.log(`[SST:${sourceName}] Grid OK: ${uniqLats.size} lats x ${uniqLons.size} lons = ${uniqLats.size*uniqLons.size} cells for ${N} points.`);}

// ── Ocean mask ────────────────────────────────────────────────────────────────
function pointInRing(px,py,ring){let inside=false;for(let i=0,j=ring.length-1;i<ring.length;j=i++){const xi=ring[i][0],yi=ring[i][1],xj=ring[j][0],yj=ring[j][1];if((yi>py)!==(yj>py)&&px<((xj-xi)*(py-yi))/(yj-yi)+xi)inside=!inside;}return inside;}
const OCEAN_MASK_URL="https://raw.githubusercontent.com/jlintvet/SSTv2/main/DailySSTData/ocean_mask.json";
async function loadPrebakedMask(){try{const t0=performance.now();const res=await fetch(OCEAN_MASK_URL);if(!res.ok){console.warn("[MASK] prebaked not available, HTTP",res.status);return null;}const obj=await res.json();const{bounds,step,rows,cols,packed}=obj;const bin=atob(packed);const bits=new Uint8Array(bin.length);for(let i=0;i<bin.length;i++)bits[i]=bin.charCodeAt(i);console.log(`[MASK] prebaked loaded in ${(performance.now()-t0).toFixed(0)}ms (${rows}x${cols}, ${bits.length} bytes)`);return(lat,lon)=>{const ri=Math.round((bounds.n-lat)/step);const ci=Math.round((lon-bounds.w)/step);if(ri<0||ri>=rows||ci<0||ci>=cols)return false;const idx=ri*cols+ci;return(bits[idx>>3]&(0x80>>(idx&7)))!==0;};}catch(e){console.warn("[MASK] prebaked load failed:",e);return null;}}
async function buildOceanMaskFromLand(bounds){const prebaked=await loadPrebakedMask();if(prebaked)return prebaked;console.warn("[MASK] falling back to live Natural Earth download");try{const res=await fetch("https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_land.geojson");const gj=await res.json();let polys=[];for(const f of gj.features){const g=f.geometry;if(g.type==="Polygon")polys.push(g.coordinates);else if(g.type==="MultiPolygon")g.coordinates.forEach(p=>polys.push(p));}polys=polys.filter(poly=>{const r=poly[0];let mnLon=Infinity,mxLon=-Infinity,mnLat=Infinity,mxLat=-Infinity;for(const[lo,la]of r){if(lo<mnLon)mnLon=lo;if(lo>mxLon)mxLon=lo;if(la<mnLat)mnLat=la;if(la>mxLat)mxLat=la;}return mxLon>=bounds.west&&mnLon<=bounds.east&&mxLat>=bounds.south&&mnLat<=bounds.north;});if(!polys.length)return null;const STEP=0.02;const ocean=new Set();for(let lat=bounds.south;lat<=bounds.north+STEP*0.5;lat+=STEP){for(let lon=bounds.west;lon<=bounds.east+STEP*0.5;lon+=STEP){let isLand=false;for(const poly of polys){if(pointInRing(lon,lat,poly[0])){let inHole=false;for(let h=1;h<poly.length;h++){if(pointInRing(lon,lat,poly[h])){inHole=true;break;}}if(!inHole){isLand=true;break;}}}if(!isLand)ocean.add(`${Math.round((lat-bounds.south)/STEP)}_${Math.round((lon-bounds.west)/STEP)}`);}}if(!ocean.size)return null;return(lat,lon)=>ocean.has(`${Math.round((lat-bounds.south)/STEP)}_${Math.round((lon-bounds.west)/STEP)}`);}catch(e){console.error("[MASK] fallback also failed:",e);return null;}}

// ── gridToDataURL ─────────────────────────────────────────────────────────────
function gridToDataURL(latSet,lonSet,grid,valMin,valMax,colorFn,isOcean){
  if(!latSet.length||!lonSet.length)return null;
  const latNorth=latSet[0],latSouth=latSet[latSet.length-1],lonWest=lonSet[0],lonEast=lonSet[lonSet.length-1];
  const lonRange=lonEast-lonWest||1;
  const CANVAS_W=512,CANVAS_H=400;const canvas=document.createElement("canvas");canvas.width=CANVAS_W;canvas.height=CANVAS_H;
  const ctx=canvas.getContext("2d");const img=ctx.createImageData(CANVAS_W,CANVAS_H);const d=img.data;
  const latStep=latSet.length>1?(latNorth-latSouth)/(latSet.length-1):0.05;const lonStep=lonSet.length>1?(lonEast-lonWest)/(lonSet.length-1):0.05;
  const mercY=(lat)=>Math.log(Math.tan(Math.PI/4+(lat*Math.PI/180)/2));const invMercY=(y)=>(2*Math.atan(Math.exp(y))-Math.PI/2)*180/Math.PI;
  const mercYNorth=mercY(latNorth),mercYSouth=mercY(latSouth),mercYRange=mercYNorth-mercYSouth||1;
  for(let py=0;py<CANVAS_H;py++){const mY=mercYNorth-(py/(CANVAS_H-1))*mercYRange;const lat=invMercY(mY);const latFloat=(latNorth-lat)/latStep;const latIdx0=Math.max(0,Math.min(latSet.length-2,Math.floor(latFloat)));const latFrac=Math.max(0,Math.min(1,latFloat-latIdx0));const gridLat0=latSet[latIdx0],gridLat1=latSet[latIdx0+1];
    for(let px=0;px<CANVAS_W;px++){const lon=lonWest+(px/(CANVAS_W-1))*lonRange;if(isOcean&&!isOcean(lat,lon))continue;const lonFloat=(lon-lonWest)/lonStep;const lonIdx0=Math.max(0,Math.min(lonSet.length-2,Math.floor(lonFloat)));const lonFrac=Math.max(0,Math.min(1,lonFloat-lonIdx0));const gridLon0=lonSet[lonIdx0],gridLon1=lonSet[lonIdx0+1];const vNW=grid[`${gridLat0}_${gridLon0}`],vNE=grid[`${gridLat0}_${gridLon1}`];const vSW=grid[`${gridLat1}_${gridLon0}`],vSE=grid[`${gridLat1}_${gridLon1}`];const wNW=(1-latFrac)*(1-lonFrac),wNE=(1-latFrac)*lonFrac,wSW=latFrac*(1-lonFrac),wSE=latFrac*lonFrac;let sum=0,wsum=0;if(vNW!=null&&Number.isFinite(vNW)){sum+=vNW*wNW;wsum+=wNW;}if(vNE!=null&&Number.isFinite(vNE)){sum+=vNE*wNE;wsum+=wNE;}if(vSW!=null&&Number.isFinite(vSW)){sum+=vSW*wSW;wsum+=wSW;}if(vSE!=null&&Number.isFinite(vSE)){sum+=vSE*wSE;wsum+=wSE;}if(wsum<0.25)continue;const val=sum/wsum;const rgb=colorFn?colorFn(val,valMin,valMax):sstColor(val,valMin,valMax);if(!rgb)continue;const i=(py*CANVAS_W+px)*4;d[i]=rgb[0];d[i+1]=rgb[1];d[i+2]=rgb[2];d[i+3]=220;}}
  ctx.putImageData(img,0,0);
  return new Promise((resolve)=>{canvas.toBlob((blob)=>{if(!blob){resolve(null);return;}resolve({dataURL:URL.createObjectURL(blob),west:lonWest-lonStep/2,east:lonEast+lonStep/2,north:latNorth+latStep/2,south:latSouth-latStep/2});},"image/png");});
}

// ─────────────────────────────────────────────────────────────────────────────
// SSTHeatmapLeaflet
// ─────────────────────────────────────────────────────────────────────────────
function SSTHeatmapLeaflet(props) {
  const {
    data, sstMin, sstMax, date, onLocationSaved, clearMarkersRef, flyToRef,
    onHoverSst, dataSource, setDataSource, activeDataLayer, setActiveDataLayer,
    chlData, chlDateIndex, setChlDateIndex, chlLoading,
    seaColorData, seaColorDateIndex, setSeaColorDateIndex, seaColorLoading,
    isMapExpanded, setIsMapExpanded,
    viirsData, viirsDateIndex, setViirsDateIndex, viirsHour, setViirsHour,
    viirsNppData, viirsNppDateIndex, setViirsNppDateIndex, activeViirsNppDay,
    murData, murDateIndex, setMurDateIndex,
    goesCompData, goesCompDateIndex, setGoesCompDateIndex, activeGoesCompDay,
    highlightedLocation, setHighlightedLocation,
    regionConfig,
    selectedLocation, setSelectedLocation,
    savedLocations, fetchSavedLocations,
    // Wind props
    windData, windLoading, windHourIndex, setWindHourIndex,
    showWindOverlay, setShowWindOverlay,
    windPlaying, setWindPlaying,
  } = props;

  const { latSet, lonSet, grid } = data;
  const regionBounds     = regionConfig.bounds;
  const REGION_LOCATIONS = regionConfig.locations;
  const llBounds = L.latLngBounds(
    [regionBounds.south, regionBounds.west],
    [regionBounds.north, regionBounds.east]
  );

  // Refs
  const mapDivRef         = useRef(null);
  const mapRef            = useRef(null);
  const sstOverlayRef     = useRef(null);
  const overlayLayerRef   = useRef(null);
  const isothermLayerRef  = useRef(null);
  const breakLayerRef     = useRef(null);
  const breakGlowRef      = useRef(null);
  const bathyLayerRef     = useRef(null);
  const wreckLayerRef     = useRef(null);
  const markersLayerRef   = useRef(null);
  const refMarkerRef      = useRef(null);
  const highlightLayerRef = useRef(null);
  const velocityLayerRef     = useRef(null);   // leaflet-velocity particles
  const windRasterOverlayRef = useRef(null);   // wind speed color fill raster
  const blobUrlsRef          = useRef([]);

  const selectedLocationRef = useRef(selectedLocation);
  useEffect(()=>{ selectedLocationRef.current = selectedLocation; }, [selectedLocation]);

  const activeDataLayerRef=useRef(activeDataLayer);useEffect(()=>{activeDataLayerRef.current=activeDataLayer;},[activeDataLayer]);
  const chlDataRef=useRef(chlData);useEffect(()=>{chlDataRef.current=chlData;},[chlData]);
  const chlDateIndexRef=useRef(chlDateIndex);useEffect(()=>{chlDateIndexRef.current=chlDateIndex;},[chlDateIndex]);
  const seaColorDataRef=useRef(seaColorData);useEffect(()=>{seaColorDataRef.current=seaColorData;},[seaColorData]);
  const seaColorDateIndexRef=useRef(seaColorDateIndex);useEffect(()=>{seaColorDateIndexRef.current=seaColorDateIndex;},[seaColorDateIndex]);

  const [showSSTLayer]      = useState(true);
  const [showBathyLayer,setShowBathyLayer]=useState(true);
  const [showWrecks,setShowWrecks]=useState(false);
  const [bathyData,setBathyData]=useState(null);const bathyDataRef=useRef(null);
  const [jsonContours,setJsonContours]=useState(null);const [jsonContoursLoading,setJsonContoursLoading]=useState(false);
  const [wrecksData,setWrecksData]=useState(null);const [wrecksLoading,setWrecksLoading]=useState(false);
  const [clickInfo,setClickInfo]=useState(null);const [hoverInfo,setHoverInfo]=useState(null);
  const [markers,setMarkers]=useState([]);const [selectedMarker,setSelectedMarker]=useState(null);
  const [savedWreckKeys,setSavedWreckKeys]=useState(new Set());
  const [hoveredWreck,setHoveredWreck]=useState(null);
  const [mapReady,setMapReady]=useState(false);
  const [sstReady,setSstReady]=useState(false);
  const waterMaskRef=useRef(null);
  const [waterMaskVersion,setWaterMaskVersion]=useState(0);
  const [repaintTrigger,setRepaintTrigger]=useState(0);
  const maskBuildStartedRef=useRef(false);
  const controlPanelRef=useRef(null);const isOverControlPanel=useRef(false);
  const [showIsotherm,setShowIsotherm]=useState(false);
  const [isothermalTargetTemp,setIsothermalTargetTemp]=useState(null);
  const [isothermalSensitivity,setIsothermalSensitivity]=useState(2.0);
  const effectiveTargetTemp=isothermalTargetTemp??Math.round((sstMin+sstMax)/2);
  const [interactionMode,setInteractionMode]=useState("pan");
  const interactionModeRef=useRef("pan");
  const [showSavedPanel,setShowSavedPanel]=useState(false);

  // Wind mode: "overlay" = white particles over SST; "map" = color gradient standalone
  const isWindMap = activeDataLayer === "windmap";
  const windActive = showWindOverlay || isWindMap;

  // ── Map initialisation ────────────────────────────────────────────────────
  useEffect(()=>{
    if(!mapDivRef.current||mapRef.current)return;
    const map = L.map(mapDivRef.current, {
      zoomControl:           true,
      attributionControl:    false,
      maxBounds:             llBounds,
      maxBoundsViscosity:    1.0,
      worldCopyJump:         false,
      preferCanvas:          true,
    });
    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      attribution: '&copy; OpenStreetMap, &copy; CARTO',
      subdomains:  "abcd",
      maxZoom:     19,
    }).addTo(map);
    const fillZoom = map.getBoundsZoom(llBounds, true);
    const center   = llBounds.getCenter();
    map.setView(center, fillZoom, { animate: false });
    map.setMinZoom(fillZoom);
    map.setMaxZoom(12);

    map.on("click", (e) => {
      if (selectedMarker) { setSelectedMarker(null); return; }
      const { lat, lng: lon } = e.latlng;
      if (lon < regionBounds.west || lon > regionBounds.east || lat < regionBounds.south || lat > regionBounds.north) return;
      const nearLat = latSet.reduce((a,b)=>Math.abs(b-lat)<Math.abs(a-lat)?b:a);
      const nearLon = lonSet.reduce((a,b)=>Math.abs(b-lon)<Math.abs(a-lon)?b:a);
      const sst = grid[`${nearLat}_${nearLon}`] ?? null;
      let depth_ft = null;
      if (bathyDataRef.current?.points?.length) {
        let best=null,bestDist=Infinity;
        for(const pt of bathyDataRef.current.points){const d=(pt.lat-lat)**2+(pt.lon-lon)**2;if(d<bestDist){bestDist=d;best=pt;}}
        depth_ft = best?.depth_ft ?? null;
      }
      const refLoc = selectedLocationRef.current;
      const containerPt = map.latLngToContainerPoint(e.latlng);
      setClickInfo({ lat, lon, sst, depth_ft,
        dist:    refLoc ? distanceNm(refLoc.lat, refLoc.lon, lat, lon) : null,
        bearing: refLoc ? bearingDeg(refLoc.lat, refLoc.lon, lat, lon) : null,
        locationLabel: refLoc?.label ?? null, px: containerPt.x, py: containerPt.y,
      });
    });

    map.on("mousemove", (e) => {
      if (interactionModeRef.current === "pan") { setHoverInfo(null); onHoverSst?.(null); return; }
      if (isOverControlPanel.current)            { setHoverInfo(null); onHoverSst?.(null); return; }
      const { lat, lng: lon } = e.latlng;
      if (lon < regionBounds.west || lon > regionBounds.east || lat < regionBounds.south || lat > regionBounds.north) {
        setHoverInfo(null); onHoverSst?.(null); return;
      }
      const nearLat = latSet.reduce((a,b)=>Math.abs(b-lat)<Math.abs(a-lat)?b:a);
      const nearLon = lonSet.reduce((a,b)=>Math.abs(b-lon)<Math.abs(a-lon)?b:a);
      const sst = grid[`${nearLat}_${nearLon}`] ?? null;
      let depth_ft = null;
      if (bathyDataRef.current?.points?.length) {
        let best=null,bestDist=Infinity;
        for(const pt of bathyDataRef.current.points){const d=(pt.lat-lat)**2+(pt.lon-lon)**2;if(d<bestDist){bestDist=d;best=pt;}}
        depth_ft = best?.depth_ft ?? null;
      }
      let chl=null, color_class=null, kd490=null;
      const adl = activeDataLayerRef.current;
      if (adl === "chlorophyll" && chlDataRef.current?.days?.length) {
        const day = chlDataRef.current.days[chlDateIndexRef.current] || chlDataRef.current.days[chlDataRef.current.days.length-1];
        if (day?.grid?.length) {
          const BIN=0.02; const nLat=Math.round(lat/BIN)*BIN, nLon=Math.round(lon/BIN)*BIN;
          const pt = day.grid.find(p=>Math.abs(p.lat-nLat)<BIN&&Math.abs(p.lon-nLon)<BIN);
          chl=pt?.chlorophyll??null; color_class=pt?.color_class??null;
        }
      } else if (adl === "seacolor" && seaColorDataRef.current?.days?.length) {
        const day = seaColorDataRef.current.days[seaColorDateIndexRef.current] || seaColorDataRef.current.days[seaColorDataRef.current.days.length-1];
        if (day?.grid?.length) {
          const BIN=0.02; const nLat=Math.round(lat/BIN)*BIN, nLon=Math.round(lon/BIN)*BIN;
          const pt = day.grid.find(p=>Math.abs(p.lat-nLat)<BIN&&Math.abs(p.lon-nLon)<BIN);
          kd490=pt?.kd490??null;
        }
      }
      const refLoc = selectedLocationRef.current;
      const containerPt = map.latLngToContainerPoint(e.latlng);
      setHoverInfo({ px: containerPt.x, py: containerPt.y, sst, depth_ft, chl, color_class, kd490,
        dist:    refLoc ? distanceNm(refLoc.lat, refLoc.lon, lat, lon) : null,
        bearing: refLoc ? bearingDeg(refLoc.lat, refLoc.lon, lat, lon) : null,
      });
      onHoverSst?.(sst);
    });
    map.on("mouseout", () => { setHoverInfo(null); onHoverSst?.(null); });

    mapRef.current = map;
    setMapReady(true);

    if (!maskBuildStartedRef.current) {
      maskBuildStartedRef.current = true;
      buildOceanMaskFromLand(regionBounds).then(mask => {
        if (mask) { waterMaskRef.current = mask; setWaterMaskVersion(v => v+1); }
      }).catch(e => console.error("[LEAFLET] mask build failed:", e));
    }

    return () => {
      blobUrlsRef.current.forEach(u => { try { URL.revokeObjectURL(u); } catch(_){} });
      blobUrlsRef.current = [];
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(()=>{
    interactionModeRef.current = interactionMode;
    const map = mapRef.current; if (!map) return;
    try { map.getContainer().style.cursor = interactionMode === "crosshair" ? "crosshair" : "grab"; } catch(_){}
    if (interactionMode === "pan") setHoverInfo(null);
  }, [interactionMode, mapReady]);

  useEffect(()=>{
    const map = mapRef.current;
    if (!mapReady || !map) return;
    const refit = () => {
      try {
        map.invalidateSize();
        const fillZoom = map.getBoundsZoom(llBounds, true);
        map.setView(llBounds.getCenter(), fillZoom, { animate: false });
        map.setMinZoom(fillZoom);
        setRepaintTrigger(t => t + 1);
      } catch(_){}
    };
    requestAnimationFrame(()=>requestAnimationFrame(refit));
    const t = setTimeout(refit, 200);
    return ()=>clearTimeout(t);
  }, [isMapExpanded, mapReady]);

  // ── SST raster overlay ────────────────────────────────────────────────────
  useEffect(()=>{
    const map = mapRef.current;
    if (!mapReady || !map || !latSet.length) return;
    const mask = waterMaskRef.current;
    if (!mask) return;
    if (sstOverlayRef.current) { map.removeLayer(sstOverlayRef.current); sstOverlayRef.current = null; }
    // Hide SST raster in wind map mode
    if (!showSSTLayer || activeDataLayer !== "sst") return;

    let cancelled = false;
    Promise.resolve(gridToDataURL(latSet, lonSet, grid, sstMin, sstMax, null, mask)).then(result => {
      if (cancelled || !result) return;
      const { dataURL, west, east, north, south } = result;
      blobUrlsRef.current.push(dataURL);
      const opacity = (dataSource === "VIIRS" || dataSource === "VIIRSSNPP" || dataSource === "GOESCOMP") ? 0.78 : 0.92;
      const overlay = L.imageOverlay(dataURL, [[south, west], [north, east]], { opacity, interactive: false });
      overlay.addTo(map);
      sstOverlayRef.current = overlay;
      setSstReady(true);
    });
    return () => { cancelled = true; };
  }, [mapReady, latSet, lonSet, grid, sstMin, sstMax, showSSTLayer, activeDataLayer, dataSource, waterMaskVersion, repaintTrigger]);

  // ── Chlorophyll / Sea color overlay ──────────────────────────────────────
  function expandCoarseGrid(latSet2,lonSet2,overlayGrid,targetLatSet,targetLonSet){const expanded={};for(const lat of targetLatSet){let r0=0;for(let i=0;i<latSet2.length-1;i++){if(lat<=latSet2[i]&&lat>=latSet2[i+1]){r0=i;break;}}const r1=Math.min(r0+1,latSet2.length-1);const latFrac=latSet2[r0]===latSet2[r1]?0:(latSet2[r0]-lat)/(latSet2[r0]-latSet2[r1]);for(const lon of targetLonSet){let c0=0;for(let i=0;i<lonSet2.length-1;i++){if(lon>=lonSet2[i]&&lon<=lonSet2[i+1]){c0=i;break;}}const c1=Math.min(c0+1,lonSet2.length-1);const lonFrac=lonSet2[c0]===lonSet2[c1]?0:(lon-lonSet2[c0])/(lonSet2[c1]-lonSet2[c0]);const vNW=overlayGrid[`${latSet2[r0]}_${lonSet2[c0]}`],vNE=overlayGrid[`${latSet2[r0]}_${lonSet2[c1]}`];const vSW=overlayGrid[`${latSet2[r1]}_${lonSet2[c0]}`],vSE=overlayGrid[`${latSet2[r1]}_${lonSet2[c1]}`];const wNW=(1-latFrac)*(1-lonFrac),wNE=(1-latFrac)*lonFrac,wSW=latFrac*(1-lonFrac),wSE=latFrac*lonFrac;let sum=0,wsum=0;if(vNW!=null&&Number.isFinite(vNW)){sum+=vNW*wNW;wsum+=wNW;}if(vNE!=null&&Number.isFinite(vNE)){sum+=vNE*wNE;wsum+=wNE;}if(vSW!=null&&Number.isFinite(vSW)){sum+=vSW*wSW;wsum+=wSW;}if(vSE!=null&&Number.isFinite(vSE)){sum+=vSE*wSE;wsum+=wSE;}if(wsum>=0.25)expanded[`${lat}_${lon}`]=sum/wsum;}}return expanded;}

  useEffect(()=>{
    const map = mapRef.current;
    if (!mapReady || !map) return;
    if (overlayLayerRef.current) { map.removeLayer(overlayLayerRef.current); overlayLayerRef.current = null; }
    let overlayGrid=null,latSet2=[],lonSet2=[],colorFn=null,min2=0,max2=1;
    if (activeDataLayer === "chlorophyll" && chlData?.days?.length) {
      const day = chlData.days[chlDateIndex] || chlData.days[chlData.days.length-1];
      if (!day?.grid?.length) return;
      latSet2=[...new Set(day.grid.map(d=>d.lat))].sort((a,b)=>b-a);
      lonSet2=[...new Set(day.grid.map(d=>d.lon))].sort((a,b)=>a-b);
      overlayGrid={};day.grid.forEach(d=>{overlayGrid[`${d.lat}_${d.lon}`]=d.chlorophyll;});
      min2=day.stats.min;max2=day.stats.max;colorFn=chlColor;
    } else if (activeDataLayer === "seacolor" && seaColorData?.days?.length) {
      const day = seaColorData.days[seaColorDateIndex] || seaColorData.days[seaColorData.days.length-1];
      if (!day?.grid?.length) return;
      latSet2=[...new Set(day.grid.map(d=>d.lat))].sort((a,b)=>b-a);
      lonSet2=[...new Set(day.grid.map(d=>d.lon))].sort((a,b)=>a-b);
      overlayGrid={};day.grid.forEach(d=>{overlayGrid[`${d.lat}_${d.lon}`]=d.kd490;});
      min2=day.stats.min;max2=day.stats.max;colorFn=kd490Color;
    } else { return; }
    if (!latSet2.length) return;
    let cancelled=false;
    const renderLatSet = activeDataLayer === "seacolor" ? latSet : latSet2;
    const renderLonSet = activeDataLayer === "seacolor" ? lonSet : lonSet2;
    const renderGrid   = activeDataLayer === "seacolor" ? expandCoarseGrid(latSet2,lonSet2,overlayGrid,latSet,lonSet) : overlayGrid;
    Promise.resolve(gridToDataURL(renderLatSet,renderLonSet,renderGrid,min2,max2,colorFn,waterMaskRef.current)).then(result=>{
      if (cancelled || !result) return;
      const { dataURL, west, east, north, south } = result;
      blobUrlsRef.current.push(dataURL);
      const overlay = L.imageOverlay(dataURL, [[south, west], [north, east]], { opacity: 0.92, interactive: false });
      overlay.addTo(map);
      overlayLayerRef.current = overlay;
    });
    return ()=>{cancelled=true;};
  }, [mapReady, activeDataLayer, chlData, chlDateIndex, seaColorData, seaColorDateIndex, waterMaskVersion, repaintTrigger]);

  // ── Wind velocity layer ───────────────────────────────────────────────────
  useEffect(() => {
    const map = mapRef.current;
    if (!mapReady || !map) return;

    // Remove existing layers
    if (velocityLayerRef.current) { map.removeLayer(velocityLayerRef.current); velocityLayerRef.current = null; }
    if (windRasterOverlayRef.current) { map.removeLayer(windRasterOverlayRef.current); windRasterOverlayRef.current = null; }

    if (!windActive || !windData?.hours?.length) return;

    // Wait for leaflet-velocity CDN script to load
    if (!L.velocityLayer) {
      const t = setTimeout(() => setRepaintTrigger(p => p + 1), 500);
      return () => clearTimeout(t);
    }

    const hourData = windData.hours[windHourIndex] ?? windData.hours[0];
    if (!hourData?.velocityJSON) return;

    const isOverlay = showWindOverlay && !isWindMap;
    const maxSpd    = windData.maxSpeed ?? 30;

    // ── Wind Map mode: color raster fill + white direction particles ──────────
    if (isWindMap && hourData.grid?.length) {
      // Build speed raster grid
      const gridPts  = hourData.grid;
      const rLats    = [...new Set(gridPts.map(p => p.lat))].sort((a,b) => b - a);
      const rLons    = [...new Set(gridPts.map(p => p.lon))].sort((a,b) => a - b);
      const speedGrid = {};
      gridPts.forEach(p => { speedGrid[`${p.lat}_${p.lon}`] = p.speed ?? Math.sqrt(p.u**2 + p.v**2); });

      // Render async — gridToDataURL returns a Promise
      Promise.resolve(gridToDataURL(rLats, rLons, speedGrid, 0, maxSpd, windSpeedColor, null)).then(result => {
        if (!result || !mapRef.current) return;
        const { dataURL, west, east, north, south } = result;
        blobUrlsRef.current.push(dataURL);
        const raster = L.imageOverlay(dataURL, [[south, west], [north, east]], { opacity: 0.82, interactive: false });
        raster.addTo(mapRef.current);
        windRasterOverlayRef.current = raster;
      });
    }

    // ── Particle layer — white in both modes ──────────────────────────────────
    const whiteScale = [
      "rgba(255,255,255,0.4)",
      "rgba(255,255,255,0.65)",
      "rgba(255,255,255,0.85)",
      "rgba(255,255,255,0.95)",
    ];

    const velocityLayer = L.velocityLayer({
      displayValues: true,
      displayOptions: {
        velocityType:    "Wind",
        position:        "bottomleft",
        emptyString:     "No wind data",
        angleConvention: "meteoCW",
        showCardinal:    true,
        speedUnit:       "kt",
        directionString: "Direction",
        speedString:     "Speed",
      },
      data:               hourData.velocityJSON,
      minVelocity:        0,
      maxVelocity:        maxSpd,
      velocityScale:      0.005,
      colorScale:         whiteScale,  // always white — color comes from raster in wind map mode
      opacity:            isOverlay ? 0.65 : 0.85,
      particleAge:        40,
      particleMultiplier: 0.0008,
      lineWidth:          isOverlay ? 1.8 : 2.0,
    });

    velocityLayer.addTo(map);
    velocityLayerRef.current = velocityLayer;

    return () => {
      if (velocityLayerRef.current) { map.removeLayer(velocityLayerRef.current); velocityLayerRef.current = null; }
      if (windRasterOverlayRef.current) { map.removeLayer(windRasterOverlayRef.current); windRasterOverlayRef.current = null; }
    };
  }, [mapReady, windActive, windData, windHourIndex, showWindOverlay, isWindMap, repaintTrigger]);

  // ── Isotherm + temp-break ─────────────────────────────────────────────────
  useEffect(()=>{
    const map = mapRef.current;
    if (!mapReady || !map) return;
    [isothermLayerRef, breakLayerRef, breakGlowRef].forEach(r => {
      if (r.current) { map.removeLayer(r.current); r.current = null; }
    });
    if (!showIsotherm || !latSet.length || activeDataLayer !== "sst") return;
    const tid = setTimeout(()=>{
      try {
        const { isotherms, breaks } = buildIsothermLines(latSet, lonSet, grid, effectiveTargetTemp, isothermalSensitivity);
        if (isotherms.length) {
          const lyr = L.layerGroup();
          isotherms.forEach(line => L.polyline(line, { color: "rgba(255,255,255,0.65)", weight: 1.5, dashArray: "3 4", interactive: false }).addTo(lyr));
          lyr.addTo(map); isothermLayerRef.current = lyr;
        }
        if (breaks.length) {
          const glow = L.layerGroup();
          breaks.forEach(line => L.polyline(line, { color: "rgba(0,207,255,0.35)", weight: 7, opacity: 1.0, interactive: false }).addTo(glow));
          glow.addTo(map); breakGlowRef.current = glow;
          const main = L.layerGroup();
          breaks.forEach(line => L.polyline(line, { color: "#00cfff", weight: 2.5, opacity: 0.97, interactive: false }).addTo(main));
          main.addTo(map); breakLayerRef.current = main;
        }
      } catch(err) { console.error("[ISOTHERM] computation failed:", err); }
    }, 60);
    return ()=>clearTimeout(tid);
  }, [mapReady, showIsotherm, latSet, lonSet, grid, effectiveTargetTemp, isothermalSensitivity, activeDataLayer, waterMaskVersion, repaintTrigger]);

  // ── Bathymetry ────────────────────────────────────────────────────────────
  useEffect(()=>{
    if (!sstReady || !showBathyLayer || jsonContours) return;
    setJsonContoursLoading(true);
    fetch(BATHY_CONTOURS_URL).then(r=>r.json()).then(d=>{setJsonContours(d);setJsonContoursLoading(false);}).catch(()=>setJsonContoursLoading(false));
  }, [sstReady, showBathyLayer]);

  useEffect(()=>{
    const map = mapRef.current;
    if (!mapReady || !map) return;
    if (bathyLayerRef.current) { map.removeLayer(bathyLayerRef.current); bathyLayerRef.current = null; }
    if (!showBathyLayer || !jsonContours) return;
    const lyr = L.geoJSON(jsonContours, {
      interactive: false,
      style: f => {
        const d = f.properties.depth_ft;
        const weight = (d===100||d===300||d===600) ? 1.5 : 0.7;
        return { color: "rgba(0,0,0,0.85)", weight, opacity: 0.85 };
      },
    });
    lyr.addTo(map); bathyLayerRef.current = lyr;
  }, [mapReady, showBathyLayer, jsonContours]);

  // ── Wrecks ────────────────────────────────────────────────────────────────
  useEffect(()=>{
    if (!showWrecks || wrecksData) return;
    setWrecksLoading(true);
    fetch(WRECKS_URL).then(r=>r.json()).then(d=>{setWrecksData(d);setWrecksLoading(false);}).catch(()=>setWrecksLoading(false));
  }, [showWrecks]);

  useEffect(()=>{
    const map = mapRef.current;
    if (!mapReady || !map) return;
    if (wreckLayerRef.current) { map.removeLayer(wreckLayerRef.current); wreckLayerRef.current = null; }
    if (!showWrecks || !wrecksData) return;
    const loc = selectedLocationRef.current;
    const lyr = L.layerGroup();
    wrecksData.features.forEach(f => {
      const [lon, lat] = f.geometry.coordinates;
      const props = f.properties || {};
      if (lat<regionBounds.south||lat>regionBounds.north||lon<regionBounds.west||lon>regionBounds.east) return;
      if (loc?.wreckRegion && props.region && props.region !== loc.wreckRegion) return;
      const m = L.circleMarker([lat, lon], { radius:5, color:"#fff", weight:1, fillColor:props.symbol==="Wreck"?"#ef4444":"#f59e0b", fillOpacity:0.9 });
      m.on("mouseover", e => { const containerPt=map.latLngToContainerPoint(e.latlng); setHoveredWreck({px:containerPt.x,py:containerPt.y,props,lat,lon}); try{map.getContainer().style.cursor="pointer";}catch(_){} });
      m.on("mouseout", () => { setHoveredWreck(null); try{map.getContainer().style.cursor=interactionModeRef.current==="crosshair"?"crosshair":"grab";}catch(_){} });
      m.addTo(lyr);
    });
    lyr.addTo(map); wreckLayerRef.current = lyr;
  }, [mapReady, showWrecks, wrecksData, selectedLocation, regionBounds]);

  // ── Saved markers ─────────────────────────────────────────────────────────
  useEffect(()=>{
    const map = mapRef.current;
    if (!mapReady || !map) return;
    if (markersLayerRef.current) { map.removeLayer(markersLayerRef.current); markersLayerRef.current = null; }
    if (!markers.length) return;
    const lyr = L.layerGroup();
    markers.forEach((mk, i) => {
      const m = L.circleMarker([mk.lat, mk.lon], { radius:7, color:"#fff", weight:1.5, fillColor:"#f97316", fillOpacity:1 });
      m.on("click", e => { L.DomEvent.stopPropagation(e); const containerPt=map.latLngToContainerPoint(e.latlng); setSelectedMarker({px:containerPt.x,py:containerPt.y,mk:{...mk,index:i}}); setClickInfo(null); });
      m.addTo(lyr);
    });
    lyr.addTo(map); markersLayerRef.current = lyr;
  }, [mapReady, markers]);

  // ── Reference location marker ─────────────────────────────────────────────
  useEffect(()=>{
    const map = mapRef.current;
    if (!mapReady || !map) return;
    if (refMarkerRef.current) { map.removeLayer(refMarkerRef.current); refMarkerRef.current = null; }
    if (!selectedLocation) return;
    const icon = L.divIcon({ className:"", html:'<div style="width:14px;height:14px;background:#3b82f6;border:2px solid white;border-radius:50%;box-shadow:0 1px 4px rgba(0,0,0,0.4);"></div>', iconSize:[14,14], iconAnchor:[7,7] });
    const m = L.marker([selectedLocation.lat, selectedLocation.lon], { icon }).bindPopup(selectedLocation.label);
    m.addTo(map); refMarkerRef.current = m;
  }, [mapReady, selectedLocation]);

  // ── Bathy depth lookup ────────────────────────────────────────────────────
  useEffect(()=>{
    if (!sstReady) return;
    fetch(BATHY_URL).then(r=>r.json()).then(d=>{ setBathyData(d); bathyDataRef.current = d; }).catch(()=>{});
  }, [sstReady]);

  // ── Highlight ring ────────────────────────────────────────────────────────
  useEffect(()=>{
    const map = mapRef.current;
    if (!mapReady || !map) return;
    if (highlightLayerRef.current) { map.removeLayer(highlightLayerRef.current); highlightLayerRef.current = null; }
    if (!highlightedLocation) return;
    const m = L.circleMarker([highlightedLocation.lat, highlightedLocation.lon], { radius:14, color:"#00BFFF", weight:3, fillOpacity:0, interactive:false });
    m.addTo(map); highlightLayerRef.current = m;
  }, [mapReady, highlightedLocation]);

  // ── Imperative refs ───────────────────────────────────────────────────────
  useEffect(()=>{
    if (flyToRef) flyToRef.current = (lat, lon) => {
      const map = mapRef.current; if (!map) return;
      map.setView([lat, lon], Math.max(map.getZoom(), 8), { animate: true });
    };
  }, [flyToRef]);
  useEffect(()=>{
    if (clearMarkersRef) clearMarkersRef.current = id => {
      if (id === null) { setMarkers([]); setSelectedMarker(null); }
      else { setMarkers(m => m.filter(mk => mk.id !== id)); setSelectedMarker(sm => sm?.mk?.id === id ? null : sm); }
    };
  }, [clearMarkersRef]);

  // ── Determine bottom padding for time slider ──────────────────────────────
  // When wind is active, the slider bar takes ~80px at the bottom.
  const sliderHeight = windActive ? 80 : 0;

  return (
    <div className={`bg-white rounded-xl border border-slate-200 shadow-sm ${isMapExpanded ? "flex flex-col h-full p-0 overflow-hidden" : "p-2 sm:p-3"}`}>
      <div className="flex items-center justify-between mb-2">
        <h2 className="text-xs font-semibold text-slate-600">
          {isWindMap
            ? <><Wind className="inline w-3.5 h-3.5 mr-1 text-cyan-500"/><span className="text-cyan-600 font-bold">Wind Map</span>{windData?.hours?.[windHourIndex]?.time && <span className="ml-2 text-slate-400 font-normal text-[10px]">{formatWindTime(windData.hours[windHourIndex].time)}</span>}</>
            : activeDataLayer==="chlorophyll"
              ? <><span>Chlorophyll -- {chlData?.days?.[chlDateIndex]?.date??"..."}</span><span className="text-green-600 ml-1">(mg/m3)</span></>
              : activeDataLayer==="seacolor"
                ? <><span>Sea Color (Kd490) -- {seaColorData?.days?.[seaColorDateIndex]?.date??"..."}</span><span className="text-teal-600 ml-1">(m-1)</span></>
                : <><span>SST -- {date??"..."}</span>
                    {dataSource==="VIIRS"?<span className="ml-1.5 text-violet-600">(VIIRS Passes)</span>:dataSource==="VIIRSSNPP"?<span className="ml-1.5 text-orange-600">(VIIRS Daily)</span>:dataSource==="GOESCOMP"?<span className="ml-1.5 text-indigo-600">(GOES Composite)</span>:<span className="ml-1.5 text-cyan-600">(MUR Daily)</span>}
                    {showWindOverlay&&<span className="ml-2 text-cyan-400 font-normal text-[10px]">+ Wind overlay</span>}
                  </>
          }
          {showIsotherm&&activeDataLayer==="sst"&&<span className="ml-2 text-sky-500 font-normal">{effectiveTargetTemp.toFixed(1)}F break</span>}
        </h2>
        <Badge variant="outline" className="text-xs text-slate-500 border-slate-300 bg-slate-50">{latSet.length} x {lonSet.length} pts</Badge>
      </div>

      <div className={`relative bg-slate-100 rounded overflow-hidden${isMapExpanded ? " flex-1 flex flex-col" : ""}`}>
        <div
          ref={mapDivRef}
          className={`rounded overflow-hidden${isMapExpanded?" flex-1":""}`}
          style={isMapExpanded
            ? { background: "transparent", width: "100%", height: `calc(100vh - 90px - ${sliderHeight}px)` }
            : { background: "transparent", width: "100%", height: `calc(70vh - ${sliderHeight}px)`, maxHeight: "100%" }}
        />

        {/* Desktop control panel */}
        <div ref={controlPanelRef}
          onPointerEnter={()=>{isOverControlPanel.current=true;setHoverInfo(null);}}
          onPointerLeave={()=>{isOverControlPanel.current=false;}}
          className="hidden sm:flex absolute right-2 top-2 z-[1000] flex-col gap-1.5 items-stretch bg-white/90 backdrop-blur-sm border border-slate-200 rounded-xl p-2 shadow-md overflow-y-auto"
          style={{width:152,maxHeight:"calc(100% - 16px)"}}>

          <select value={selectedLocation?.label||""} onChange={e=>setSelectedLocation(REGION_LOCATIONS.find(l=>l.label===e.target.value))} className="bg-white border border-slate-300 text-slate-700 text-[11px] rounded-lg px-2 py-1.5 focus:outline-none w-full">
            {REGION_LOCATIONS.map(l=><option key={l.label} value={l.label}>{l.label}</option>)}
          </select>

          <div className="border-t border-slate-200 my-0.5"/>
          <div className="text-[10px] text-slate-400 font-semibold uppercase tracking-wide px-1">Map Mode</div>
          <div className="grid grid-cols-2 gap-1">
            <button onClick={()=>setInteractionMode("crosshair")} className={`flex items-center justify-center gap-1 text-[11px] font-semibold px-2 py-1.5 rounded-lg border transition-colors ${interactionMode==="crosshair"?"bg-cyan-600 text-white border-cyan-500":"bg-white text-slate-600 border-slate-300 hover:bg-slate-50"}`}><Crosshair className="w-3.5 h-3.5"/><span>Inspect</span></button>
            <button onClick={()=>setInteractionMode("pan")} className={`flex items-center justify-center gap-1 text-[11px] font-semibold px-2 py-1.5 rounded-lg border transition-colors ${interactionMode==="pan"?"bg-slate-700 text-white border-slate-600":"bg-white text-slate-600 border-slate-300 hover:bg-slate-50"}`}><Move className="w-3.5 h-3.5"/><span>Pan</span></button>
          </div>

          <div className="border-t border-slate-200 my-0.5"/>
          <div className="text-[10px] text-slate-400 font-semibold uppercase tracking-wide px-1">Data Layer</div>
          <button onClick={()=>setActiveDataLayer("sst")} className={`text-[11px] font-semibold px-2 py-1.5 rounded-lg text-left ${activeDataLayer==="sst"?"bg-cyan-600 text-white":"bg-white text-slate-600 hover:bg-slate-50 border border-slate-300"}`}>SST</button>
          <button onClick={()=>setActiveDataLayer("chlorophyll")} className={`text-[11px] font-semibold px-2 py-1.5 rounded-lg text-left ${activeDataLayer==="chlorophyll"?"bg-green-600 text-white":"bg-white text-slate-600 hover:bg-slate-50 border border-slate-300"}`}>{chlLoading?"Loading...":"Chlorophyll"}</button>
          <button onClick={()=>setActiveDataLayer("seacolor")} className={`text-[11px] font-semibold px-2 py-1.5 rounded-lg text-left ${activeDataLayer==="seacolor"?"bg-teal-600 text-white":"bg-white text-slate-600 hover:bg-slate-50 border border-slate-300"}`}>{seaColorLoading?"Loading...":seaColorData&&!seaColorData.days?.length?"Sea Color (no data)":"Sea Color"}</button>
          <button onClick={()=>setActiveDataLayer("windmap")} className={`text-[11px] font-semibold px-2 py-1.5 rounded-lg text-left flex items-center gap-1 ${isWindMap?"bg-sky-600 text-white":"bg-white text-slate-600 hover:bg-slate-50 border border-slate-300"}`}>
            <Wind className="w-3 h-3"/>{windLoading?"Loading...":"Wind Map"}
          </button>

          <div className="border-t border-slate-200 my-0.5"/>

          {/* Wind overlay toggle — only shown when NOT in wind map mode */}
          {!isWindMap && (
            <button
              onClick={()=>setShowWindOverlay(v=>!v)}
              className={`text-[11px] font-semibold px-2 py-1.5 rounded-lg text-left flex items-center gap-1 border transition-colors ${showWindOverlay?"bg-cyan-500 text-white border-cyan-400":"bg-white text-slate-600 border-slate-300 hover:bg-slate-50"}`}
            >
              <Wind className="w-3 h-3"/>
              {windLoading?"Loading...":showWindOverlay?"Wind On":"Wind Overlay"}
            </button>
          )}

          <button onClick={()=>setShowBathyLayer(b=>!b)} className={`text-[11px] font-semibold px-2 py-1.5 rounded-lg text-left ${showBathyLayer?"bg-blue-600 text-white":"bg-white text-slate-600 hover:bg-slate-50 border border-slate-300"}`}>{jsonContoursLoading?"Loading...":"Bathy"}</button>
          <button onClick={()=>setShowWrecks(w=>!w)} className={`text-[11px] font-semibold px-2 py-1.5 rounded-lg border text-left ${showWrecks?"bg-amber-500 text-white border-amber-400":"bg-white text-slate-600 border-slate-300 hover:bg-slate-50"}`}>{wrecksLoading?"Loading...":"Wrecks"}</button>

          {activeDataLayer==="sst"&&<IsothermControls enabled={showIsotherm} onToggle={()=>setShowIsotherm(v=>!v)} targetTemp={effectiveTargetTemp} onTargetTemp={t=>setIsothermalTargetTemp(t)} sensitivity={isothermalSensitivity} onSensitivity={setIsothermalSensitivity} sstMin={sstMin} sstMax={sstMax}/>}

          <div className="border-t border-slate-200 my-0.5"/>
          <button onClick={()=>setIsMapExpanded(e=>!e)} className="flex items-center gap-1.5 text-[11px] font-semibold px-2 py-1.5 rounded-lg border text-left bg-white text-slate-600 border-slate-300 hover:bg-slate-50">{isMapExpanded?<><Minimize2 className="w-3 h-3"/>Collapse</>:<><Maximize2 className="w-3 h-3"/>Expand</>}</button>

          {/* Date paginators */}
          {activeDataLayer==="sst"&&dataSource==="VIIRS"&&viirsData?.days?.length>=1&&(<><div className="flex items-center gap-1 mt-0.5"><button onClick={()=>setViirsDateIndex(i=>Math.max(0,i-1))} disabled={viirsDateIndex===0} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8249;</button><span className="flex-1 text-center text-[10px] font-semibold text-violet-700 bg-violet-50 rounded py-1">{viirsData.days[viirsDateIndex]?.date}</span><button onClick={()=>setViirsDateIndex(i=>Math.min(viirsData.days.length-1,i+1))} disabled={viirsDateIndex===viirsData.days.length-1} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8250;</button></div>{viirsData.days[viirsDateIndex]?.available_hours?.length>1&&(<div className="flex flex-wrap gap-1 mt-0.5">{viirsData.days[viirsDateIndex].available_hours.map(h=>(<button key={h} onClick={()=>setViirsHour(h)} className={`flex-1 text-[10px] font-semibold px-1 py-0.5 rounded border transition-colors ${viirsHour===h?"bg-violet-600 text-white border-violet-500":"bg-white text-slate-600 border-slate-300 hover:bg-slate-50"}`}>{h}z</button>))}</div>)}</>)}
          {activeDataLayer==="sst"&&dataSource==="MUR"&&murData?.days?.length>=1&&(<div className="flex items-center gap-1 mt-0.5"><button onClick={()=>setMurDateIndex(i=>Math.max(0,i-1))} disabled={murDateIndex===0} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8249;</button><span className="flex-1 text-center text-[10px] font-semibold text-cyan-700 bg-cyan-50 rounded py-1">{date}</span><button onClick={()=>setMurDateIndex(i=>Math.min(murData.days.length-1,i+1))} disabled={murDateIndex===murData.days.length-1} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8250;</button></div>)}
          {activeDataLayer==="sst"&&dataSource==="VIIRSSNPP"&&viirsNppData?.days?.length>=1&&(<div className="flex items-center gap-1 mt-0.5"><button onClick={()=>setViirsNppDateIndex(i=>Math.max(0,i-1))} disabled={viirsNppDateIndex===0} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8249;</button><span className="flex-1 text-center text-[10px] font-semibold text-orange-700 bg-orange-50 rounded py-1">{activeViirsNppDay?.date}</span><button onClick={()=>setViirsNppDateIndex(i=>Math.min(viirsNppData.days.length-1,i+1))} disabled={viirsNppDateIndex===viirsNppData.days.length-1} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8250;</button></div>)}
          {activeDataLayer==="sst"&&dataSource==="GOESCOMP"&&goesCompData?.days?.length>=1&&(<div className="flex items-center gap-1 mt-0.5"><button onClick={()=>setGoesCompDateIndex(i=>Math.max(0,i-1))} disabled={goesCompDateIndex===0} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8249;</button><span className="flex-1 text-center text-[10px] font-semibold text-indigo-700 bg-indigo-50 rounded py-1">{activeGoesCompDay?.date}</span><button onClick={()=>setGoesCompDateIndex(i=>Math.min(goesCompData.days.length-1,i+1))} disabled={goesCompDateIndex===goesCompData.days.length-1} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8250;</button></div>)}
          {activeDataLayer==="chlorophyll"&&chlData?.days?.length>1&&(<div className="flex items-center gap-1 mt-0.5"><button onClick={()=>setChlDateIndex(i=>Math.max(0,i-1))} disabled={chlDateIndex===0} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8249;</button><span className="flex-1 text-center text-[10px] font-semibold text-green-700 bg-green-50 rounded py-1">{chlData.days[chlDateIndex]?.date}</span><button onClick={()=>setChlDateIndex(i=>Math.min(chlData.days.length-1,i+1))} disabled={chlDateIndex===chlData.days.length-1} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8250;</button></div>)}
          {activeDataLayer==="seacolor"&&seaColorData?.days?.length>1&&(<div className="flex items-center gap-1 mt-0.5"><button onClick={()=>setSeaColorDateIndex(i=>Math.max(0,i-1))} disabled={seaColorDateIndex===0} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8249;</button><span className="flex-1 text-center text-[10px] font-semibold text-teal-700 bg-teal-50 rounded py-1">{seaColorData.days[seaColorDateIndex]?.date}</span><button onClick={()=>setSeaColorDateIndex(i=>Math.min(seaColorData.days.length-1,i+1))} disabled={seaColorDateIndex===seaColorData.days.length-1} className="px-1.5 py-1 rounded bg-white border border-slate-300 text-slate-600 text-xs font-bold disabled:opacity-30">&#8250;</button></div>)}

          {activeDataLayer==="sst"&&(<>
            <div className="border-t border-slate-200 my-0.5"/>
            <div className="text-[10px] text-slate-400 font-semibold uppercase tracking-wide px-1">SST Source</div>
            <div className="grid grid-cols-2 gap-1">
              <button onClick={()=>setDataSource("MUR")} className={`text-[11px] font-semibold px-1.5 py-1.5 rounded-lg transition-colors ${dataSource==="MUR"?"bg-cyan-600 text-white":"bg-white text-slate-600 hover:bg-slate-50 border border-slate-300"}`}>Daily Composite</button>
              <button onClick={()=>setDataSource("VIIRS")} className={`text-[11px] font-semibold px-1.5 py-1.5 rounded-lg transition-colors ${dataSource==="VIIRS"?"bg-violet-600 text-white":"bg-white text-slate-600 hover:bg-slate-50 border border-slate-300"}`}>VIIRS Passes</button>
              <button onClick={()=>setDataSource("GOESCOMP")} className={`text-[11px] font-semibold px-1.5 py-1.5 rounded-lg transition-colors ${dataSource==="GOESCOMP"?"bg-indigo-600 text-white":"bg-white text-slate-600 hover:bg-slate-50 border border-slate-300"}`}>GOES Comp</button>
            </div>
          </>)}
        </div>

        {/* Mobile bar */}
        <div className="sm:hidden absolute bottom-0 left-0 right-0 z-[1000] flex items-center gap-1.5 bg-white/90 backdrop-blur-sm border-t border-slate-200 px-2 py-1.5 shadow-lg" style={{bottom: sliderHeight}}>
          <select value={selectedLocation?.label||""} onChange={e=>setSelectedLocation(REGION_LOCATIONS.find(l=>l.label===e.target.value))} className="flex-1 bg-white border border-slate-300 text-slate-700 text-[11px] rounded-lg px-2 py-1.5 focus:outline-none min-w-0">{REGION_LOCATIONS.map(l=><option key={l.label} value={l.label}>{l.label}</option>)}</select>
          <button onClick={()=>setActiveDataLayer("sst")} className={`text-[11px] font-semibold px-2.5 py-1.5 rounded-lg flex-shrink-0 ${activeDataLayer==="sst"?"bg-cyan-600 text-white":"bg-white text-slate-600 border border-slate-300"}`}>SST</button>
          <button onClick={()=>setActiveDataLayer("chlorophyll")} className={`text-[11px] font-semibold px-2.5 py-1.5 rounded-lg flex-shrink-0 ${activeDataLayer==="chlorophyll"?"bg-green-600 text-white":"bg-white text-slate-600 border border-slate-300"}`}>CHL</button>
          <button onClick={()=>setActiveDataLayer("seacolor")} className={`text-[11px] font-semibold px-2.5 py-1.5 rounded-lg flex-shrink-0 ${activeDataLayer==="seacolor"?"bg-teal-600 text-white":"bg-white text-slate-600 border border-slate-300"}`}>SC</button>
          <button onClick={()=>setActiveDataLayer("windmap")} className={`text-[11px] font-semibold px-2.5 py-1.5 rounded-lg flex-shrink-0 ${isWindMap?"bg-sky-600 text-white":"bg-white text-slate-600 border border-slate-300"}`}>WND</button>
          <button onClick={()=>setShowWindOverlay(v=>!v)} className={`text-[11px] font-semibold px-2.5 py-1.5 rounded-lg flex-shrink-0 border ${showWindOverlay?"bg-cyan-500 text-white border-cyan-400":"bg-white text-slate-600 border-slate-300"}`}>W+</button>
          <button onClick={()=>setShowBathyLayer(b=>!b)} className={`text-[11px] font-semibold px-2.5 py-1.5 rounded-lg flex-shrink-0 ${showBathyLayer?"bg-blue-600 text-white":"bg-white text-slate-600 border border-slate-300"}`}>{jsonContoursLoading?"...":"BTH"}</button>
          <button onClick={()=>setShowWrecks(w=>!w)} className={`text-[11px] font-semibold px-2.5 py-1.5 rounded-lg border flex-shrink-0 ${showWrecks?"bg-amber-500 text-white border-amber-400":"bg-white text-slate-600 border-slate-300"}`}>WRK</button>
          {activeDataLayer==="sst"&&<button onClick={()=>setShowIsotherm(v=>!v)} className={`text-[11px] font-semibold px-2.5 py-1.5 rounded-lg border flex-shrink-0 ${showIsotherm?"bg-sky-500 text-white border-sky-400":"bg-white text-slate-600 border-slate-300"}`}>ISO</button>}
          <button onClick={()=>setIsMapExpanded(e=>!e)} className={`text-[11px] font-semibold px-2.5 py-1.5 rounded-lg flex-shrink-0 ${isMapExpanded?"bg-slate-700 text-white border border-slate-600":"bg-white text-slate-600 border border-slate-300"}`}>{isMapExpanded?"[-]":"[+]"}</button>
        </div>

        {/* Isotherm mobile sliders */}
        {showIsotherm&&activeDataLayer==="sst"&&(
          <div className="sm:hidden absolute left-0 right-0 z-[1000] bg-white/95 backdrop-blur-sm border-t border-sky-200 px-3 py-2 shadow-lg" style={{bottom: sliderHeight + 48}}>
            <div className="flex items-center gap-3">
              <div className="flex-1"><div className="flex justify-between text-[10px] text-slate-500 mb-0.5"><span>Target</span><span className="text-sky-600 font-bold">{effectiveTargetTemp.toFixed(1)}F</span></div><input type="range" min={Math.floor(sstMin)} max={Math.ceil(sstMax)} step={0.5} value={effectiveTargetTemp} onChange={e=>setIsothermalTargetTemp(parseFloat(e.target.value))} className="w-full h-1.5 rounded-full appearance-none cursor-pointer accent-sky-500"/></div>
              <div className="flex-1"><div className="flex justify-between text-[10px] text-slate-500 mb-0.5"><span>Sensitivity</span><span className="text-violet-600 font-bold">+/-{isothermalSensitivity.toFixed(1)}F</span></div><input type="range" min={0.5} max={8} step={0.5} value={isothermalSensitivity} onChange={e=>setIsothermalSensitivity(parseFloat(e.target.value))} className="w-full h-1.5 rounded-full appearance-none cursor-pointer accent-violet-500"/></div>
            </div>
          </div>
        )}

        {/* Hover tooltip */}
        {hoverInfo&&!clickInfo&&(<div className="absolute z-[1100] pointer-events-none bg-white/95 border border-slate-200 rounded-lg px-2.5 py-1.5 text-xs shadow-lg space-y-0.5" style={{left:hoverInfo.px+14,top:hoverInfo.py-10}}>{activeDataLayer==="sst"&&hoverInfo.sst!=null&&<div className="text-cyan-600 font-semibold">{hoverInfo.sst.toFixed(1)}F</div>}{activeDataLayer==="chlorophyll"&&hoverInfo.chl!=null&&<div className="text-green-600 font-semibold">{hoverInfo.chl.toFixed(3)} mg/m3 <span className="text-slate-400 font-normal">({hoverInfo.color_class})</span></div>}{activeDataLayer==="seacolor"&&hoverInfo.kd490!=null&&<div className="text-teal-600 font-semibold">{hoverInfo.kd490.toFixed(4)} m-1</div>}{hoverInfo.depth_ft!=null&&<div className="text-blue-600 font-medium">{Math.round(hoverInfo.depth_ft)} ft / {Math.round(hoverInfo.depth_ft/6)} fth</div>}{hoverInfo.dist!=null&&<div className="text-slate-600">{hoverInfo.dist.toFixed(1)} nm {Math.round(hoverInfo.bearing)} {bearingLabel(hoverInfo.bearing)}</div>}{hoverInfo.sst==null&&hoverInfo.depth_ft==null&&hoverInfo.chl==null&&hoverInfo.kd490==null&&<div className="text-slate-400">No data</div>}</div>)}
        {hoveredWreck&&(<div className="absolute z-[1100] bg-white border border-amber-300 rounded-lg px-2.5 py-2 text-xs shadow-lg min-w-40 pointer-events-none" style={{left:hoveredWreck.px+12,top:hoveredWreck.py-10}}><div className="text-amber-600 font-semibold mb-1">Wreck: {hoveredWreck.props.name||hoveredWreck.props.symbol||"Unknown"}</div><div className="text-slate-500 text-[10px]">{hoveredWreck.props.symbol}</div>{hoveredWreck.props.depth_ft!=null&&<div className="text-blue-600 font-medium">{Math.round(hoveredWreck.props.depth_ft)} ft / {Math.round(hoveredWreck.props.depth_ft/6)} fth</div>}{hoveredWreck.props.year_sunk&&<div className="text-slate-500">Sunk: {hoveredWreck.props.year_sunk}</div>}</div>)}
        {clickInfo&&(<MapClickInfo info={clickInfo} date={date} onClose={()=>setClickInfo(null)} onSaved={info=>{setMarkers(m=>[...m,{lat:info.lat,lon:info.lon,sst:info.sst,depth_ft:info.depth_ft,label:info.label,id:info.id,dist_nm:info.dist,bearing_deg:info.bearing!=null?Math.round(info.bearing):null,bearing_cardinal:info.bearing!=null?bearingLabel(info.bearing):null,from_location:info.locationLabel}]);setSavedWreckKeys(s=>new Set([...s,`${info.lat}_${info.lon}`]));onLocationSaved();setClickInfo(null);}}/>)}
        {selectedMarker&&(<div className="absolute z-[1100] bg-white border border-slate-200 rounded-xl shadow-xl p-3 w-52 text-xs" style={{left:selectedMarker.px+12,top:selectedMarker.py-40}} onClick={e=>e.stopPropagation()}><div className="flex items-center justify-between mb-2"><span className="text-slate-800 font-semibold truncate">{selectedMarker.mk.label||"Saved Location"}</span><button onClick={()=>setSelectedMarker(null)} className="text-slate-400 hover:text-slate-700 ml-2 flex-shrink-0"><svg width="14" height="14" viewBox="0 0 14 14"><path d="M10.5 3.5l-7 7M3.5 3.5l7 7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/></svg></button></div><div className="space-y-1 mb-2"><div className="flex justify-between"><span className="text-slate-500">Lat</span><span className="font-mono font-semibold">{parseFloat(selectedMarker.mk.lat).toFixed(4)}N</span></div><div className="flex justify-between"><span className="text-slate-500">Lon</span><span className="font-mono font-semibold">{parseFloat(selectedMarker.mk.lon).toFixed(4)}E</span></div>{selectedMarker.mk.sst!=null&&<div className="flex justify-between"><span className="text-slate-500">Temp</span><span className="text-cyan-600 font-semibold">{parseFloat(selectedMarker.mk.sst).toFixed(1)}F</span></div>}{selectedMarker.mk.depth_ft!=null&&<div className="flex justify-between"><span className="text-slate-500">Depth</span><span className="text-blue-600 font-semibold">{Math.round(selectedMarker.mk.depth_ft)} ft / {Math.round(selectedMarker.mk.depth_ft/6)} fth</span></div>}</div><button onClick={async()=>{const mk=selectedMarker.mk;if(mk.id)await base44.entities.SavedLocation.delete(mk.id);setMarkers(m=>m.filter(m2=>m2.id!==mk.id));setSelectedMarker(null);onLocationSaved();}} className="w-full flex items-center justify-center gap-1.5 bg-red-500 hover:bg-red-600 text-white text-xs font-semibold py-1.5 rounded-lg transition-colors shadow-sm">Delete Marker</button></div>)}

        {/* Floating saved-locations panel (expanded mode) */}
        {isMapExpanded && (
          showSavedPanel ? (
            <div className="absolute right-2 z-[1000] bg-white border border-slate-200 rounded-xl shadow-xl flex flex-col" style={{ top: 8, width: 240, maxHeight: "55%" }}>
              <div className="flex items-center justify-between px-3 py-2 border-b border-slate-200 flex-shrink-0">
                <span className="text-xs font-semibold text-slate-700">Saved Locations ({savedLocations?.length ?? 0})</span>
                <button onClick={()=>setShowSavedPanel(false)} className="text-slate-400 hover:text-slate-700"><svg width="14" height="14" viewBox="0 0 14 14"><path d="M10.5 3.5l-7 7M3.5 3.5l7 7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/></svg></button>
              </div>
              <div className="flex-1 overflow-y-auto p-2">
                <SavedLocations locations={savedLocations} onRefresh={fetchSavedLocations} onClearMarkers={id => clearMarkersRef.current?.(id)} onSelectLocation={(idx, loc) => { if (!loc) { setHighlightedLocation(null); return; } flyToRef.current?.(loc.lat, loc.lon); setHighlightedLocation(loc); }} highlightedId={highlightedLocation?.id}/>
              </div>
            </div>
          ) : (
            <button onClick={()=>setShowSavedPanel(true)} className="absolute right-2 z-[1000] bg-white border border-slate-200 rounded-full shadow-lg px-3 py-2 text-xs font-semibold text-slate-700 hover:bg-slate-50 flex items-center gap-1.5" style={{bottom: sliderHeight + 8}} title="Show saved locations">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>
              <span>{savedLocations?.length ?? 0} saved</span>
            </button>
          )
        )}

        {/* ── Wind time slider (full-width, Windy-style) ───────────────────── */}
        {windActive && windData?.hours?.length > 0 && (
          <WindTimeSlider
            windData={windData}
            windHourIndex={windHourIndex}
            setWindHourIndex={setWindHourIndex}
            isPlaying={windPlaying}
            setIsPlaying={setWindPlaying}
            isWindMap={isWindMap}
          />
        )}

        {/* Wind loading indicator */}
        {windLoading && (windActive || windData === null) && (
          <div className="absolute bottom-4 left-1/2 -translate-x-1/2 z-[1100] bg-slate-900/80 text-white text-xs px-3 py-1.5 rounded-full flex items-center gap-2">
            <div className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin"/>
            Loading wind data…
          </div>
        )}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// SSTPage
// ─────────────────────────────────────────────────────────────────────────────
function SSTPage() {
  const { daysLeft, region } = useRegionAccess();
  const regionConfig = getRegionConfig(region ?? DEFAULT_REGION);

  const [murState,      setMurState]      = useState({ data: null, dateIndex: 0 });
  const [viirsState,    setViirsState]    = useState({ data: null, dateIndex: 0, hour: null });
  const [viirsNppState, setViirsNppState] = useState({ data: null, dateIndex: 0 });
  const [goesCompState, setGoesCompState] = useState({ data: null, dateIndex: 0 });

  const murData=murState.data,murDateIndex=murState.dateIndex;
  const setMurDateIndex=fn=>setMurState(s=>({...s,dateIndex:typeof fn==="function"?fn(s.dateIndex):fn}));
  const viirsData=viirsState.data,viirsDateIndex=viirsState.dateIndex,viirsHour=viirsState.hour;
  const setViirsDateIndex=fn=>setViirsState(s=>({...s,dateIndex:typeof fn==="function"?fn(s.dateIndex):fn}));
  const setViirsHour=h=>setViirsState(s=>({...s,hour:h}));
  const viirsNppData=viirsNppState.data,viirsNppDateIndex=viirsNppState.dateIndex;
  const setViirsNppDateIndex=fn=>setViirsNppState(s=>({...s,dateIndex:typeof fn==="function"?fn(s.dateIndex):fn}));
  const goesCompData=goesCompState.data,goesCompDateIndex=goesCompState.dateIndex;
  const setGoesCompDateIndex=fn=>setGoesCompState(s=>({...s,dateIndex:typeof fn==="function"?fn(s.dateIndex):fn}));

  const [loading,setLoading]=useState(true);
  const [error,setError]=useState(null);
  const [savedLocations,setSavedLocations]=useState([]);
  const clearMarkersRef=useRef(null);const flyToRef=useRef(null);
  const [legendHoverSst,setLegendHoverSst]=useState(null);
  const [dataSource,setDataSource]=useState("MUR");
  const [isMapExpanded,setIsMapExpanded]=useState(false);
  const [activeDataLayer,setActiveDataLayer]=useState("sst");
  const [chlData,setChlData]=useState(null);
  const [chlLoading,setChlLoading]=useState(false);
  const [chlDateIndex,setChlDateIndex]=useState(0);
  const [seaColorData,setSeaColorData]=useState(null);
  const [seaColorLoading,setSeaColorLoading]=useState(false);
  const [seaColorDateIndex,setSeaColorDateIndex]=useState(0);
  const [highlightedLocation,setHighlightedLocation]=useState(null);
  const [selectedLocation, setSelectedLocation] = useState(null);
  useEffect(()=>{ if(!selectedLocation) setSelectedLocation(regionConfig.locations[0] ?? null); },[regionConfig]);

  // ── Wind state ────────────────────────────────────────────────────────────
  const [windData,     setWindData]     = useState(null);
  const [windLoading,  setWindLoading]  = useState(false);
  const [windHourIndex,setWindHourIndex]= useState(0);
  const [showWindOverlay, setShowWindOverlay] = useState(false);
  const [windPlaying,  setWindPlaying]  = useState(false);

  // Fetch wind when either wind mode is activated for the first time.
  // Data is a static file updated every 3h by the updateWindData scheduled function.
  const WIND_DATA_URL = "https://raw.githubusercontent.com/jlintvet/SSTv2/main/WindData/wind_latest.json";
  const windActive = showWindOverlay || activeDataLayer === "windmap";
  useEffect(() => {
    if (!windActive || windData || windLoading) return;
    setWindLoading(true);
    fetch(WIND_DATA_URL)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(d => {
        console.log("[WIND] fetched from GitHub:", d?.hours?.length, "hours, max", d?.maxSpeed, "kt");
        setWindData(d);
        // Seek to the hour closest to current UTC time
        if (d?.hours?.length) {
          const nowISO = new Date().toISOString().slice(0, 13); // "2026-04-30T14"
          const idx = d.hours.findIndex(h => h.time.startsWith(nowISO));
          setWindHourIndex(idx >= 0 ? idx : 0);
        }
      })
      .catch(e => console.error("[WIND] fetch failed:", e))
      .finally(() => setWindLoading(false));
  }, [windActive]);

  function handleUpgrade(){alert("Upgrade coming soon!");}

  async function fetchSavedLocations(){const locs=await base44.entities.SavedLocation.list("-created_date",100);setSavedLocations(locs);}
  useEffect(()=>{const run=()=>fetchSavedLocations();if(typeof requestIdleCallback==="function"){const h=requestIdleCallback(run,{timeout:2000});return()=>cancelIdleCallback(h);}const t=setTimeout(run,500);return()=>clearTimeout(t);},[]);

  async function fetchMUR(){setLoading(true);setError(null);try{const res=await base44.functions.invoke("sstSummary",{});const normalized=normalizeSSTResponse(res,"MUR","sst");const last=Math.max(0,(normalized.days?.length??1)-1);setMurState({data:normalized,dateIndex:last});}catch(e){console.error("[SST:MUR] fetch failed:",e);setError(e.message);}setLoading(false);}
  async function fetchVIIRS(){setLoading(true);setError(null);try{const res=await base44.functions.invoke("getVIIRSData",{});const normalized=normalizeSSTResponse(res,"VIIRS","sst");const lastIdx=Math.max(0,(normalized.days?.length??1)-1);const latestDay=normalized.days?.[lastIdx];setViirsState({data:normalized,dateIndex:lastIdx,hour:latestDay?.available_hours?.[latestDay.available_hours.length-1]??null});}catch(e){console.error("[SST:VIIRS] fetch failed:",e);setError(e.message);}setLoading(false);}
  async function fetchVIIRSNpp(){setLoading(true);setError(null);try{const res=await base44.functions.invoke("getVIIRSSSTData",{});const normalized=normalizeSSTResponse(res,"VIIRSSNPP","sst");const last=Math.max(0,(normalized.days?.length??1)-1);setViirsNppState({data:normalized,dateIndex:last});}catch(e){console.error("[SST:VIIRSSNPP] fetch failed:",e);setError(e.message);}setLoading(false);}
  async function fetchGOESComposite(){setLoading(true);setError(null);try{const res=await base44.functions.invoke("getGOESCompositeData",{});const normalized=normalizeSSTResponse(res,"GOESCOMP","sst");const last=Math.max(0,(normalized.days?.length??1)-1);setGoesCompState({data:normalized,dateIndex:last});}catch(e){console.error("[SST:GOESCOMP] fetch failed:",e);setError(e.message);}setLoading(false);}
  useEffect(()=>{if(dataSource==="MUR")fetchMUR();else if(dataSource==="VIIRS")fetchVIIRS();else if(dataSource==="VIIRSSNPP")fetchVIIRSNpp();else if(dataSource==="GOESCOMP")fetchGOESComposite();},[dataSource]);

  useEffect(()=>{if(activeDataLayer!=="chlorophyll"||chlData)return;setChlLoading(true);base44.functions.invoke("getChlorophyllData",{}).then(res=>{const normalized=normalizeSSTResponse(res,"CHL","chlorophyll");setChlData(normalized);setChlDateIndex(Math.max(0,(normalized.days?.length??1)-1));setChlLoading(false);}).catch(e=>{console.error("[SST:CHL] fetch failed:",e);setChlLoading(false);});},[activeDataLayer]);
  useEffect(()=>{if(activeDataLayer!=="seacolor"||seaColorData)return;setSeaColorLoading(true);base44.functions.invoke("getSeaColorData",{}).then(res=>{const normalized=normalizeSSTResponse(res,"SEACOLOR","kd490");setSeaColorData(normalized);if(normalized?.days?.length)setSeaColorDateIndex(normalized.days.length-1);setSeaColorLoading(false);}).catch(e=>{console.error("[SST:SEACOLOR] fetch failed:",e);setSeaColorLoading(false);});},[activeDataLayer]);

  const activeViirsDay=viirsData?.days?.[viirsDateIndex]??null;
  const activeViirsGrid=viirsHour&&activeViirsDay?.hours_cache?.[viirsHour]?activeViirsDay.hours_cache[viirsHour].grid:activeViirsDay?.grid??null;
  const activeViirsStats=viirsHour&&activeViirsDay?.hours_cache?.[viirsHour]?activeViirsDay.hours_cache[viirsHour].stats:activeViirsDay?.stats??null;
  const activeMurDay=murData?.days?.[murDateIndex]??null;
  const activeViirsNppDay=viirsNppData?.days?.[viirsNppDateIndex]??null;
  const activeGoesCompDay=goesCompData?.days?.[goesCompDateIndex]??null;

  const activeGrid=dataSource==="VIIRS"?activeViirsGrid:dataSource==="VIIRSSNPP"?activeViirsNppDay?.grid??null:dataSource==="GOESCOMP"?activeGoesCompDay?.grid??null:activeMurDay?.grid??null;
  const activeStats=dataSource==="VIIRS"?activeViirsStats:dataSource==="VIIRSSNPP"?activeViirsNppDay?.stats??null:dataSource==="GOESCOMP"?activeGoesCompDay?.stats??null:activeMurDay?.stats??null;
  const selectedDate=dataSource==="VIIRS"?activeViirsDay?.date??null:dataSource==="VIIRSSNPP"?activeViirsNppDay?.date??null:dataSource==="GOESCOMP"?activeGoesCompDay?.date??null:activeMurDay?.date??null;

  const{sstMin,sstMax}=useMemo(()=>{if((dataSource==="VIIRS"||dataSource==="VIIRSSNPP")&&activeGrid?.length){const vals=activeGrid.map(d=>d.sst).filter(v=>v!=null).sort((a,b)=>a-b);if(vals.length<10)return{sstMin:activeStats?.min??32,sstMax:activeStats?.max??85};return{sstMin:vals[Math.floor(vals.length*0.02)],sstMax:vals[Math.floor(vals.length*0.98)]};}return{sstMin:activeStats?.min??32,sstMax:activeStats?.max??85};},[activeGrid,activeStats,dataSource]);

  const heatmapData=useMemo(()=>{if(!activeGrid?.length)return{latSet:[],lonSet:[],grid:{}};const latSet=[...new Set(activeGrid.map(d=>d.lat))].sort((a,b)=>b-a);const lonSet=[...new Set(activeGrid.map(d=>d.lon))].sort((a,b)=>a-b);const grid={};activeGrid.forEach(d=>{grid[`${d.lat}_${d.lon}`]=d.sst;});return{latSet,lonSet,grid};},[activeGrid]);
  const gridHealth=useMemo(()=>{if(!activeGrid?.length)return null;const N=activeGrid.length;const ratio=(heatmapData.latSet.length*heatmapData.lonSet.length)/N;if(ratio>10)return{scattered:true,N,lats:heatmapData.latSet.length,lons:heatmapData.lonSet.length};return null;},[activeGrid,heatmapData]);

  const isWindMap = activeDataLayer === "windmap";

  return(
    <div className="h-screen bg-gradient-to-br from-blue-50 via-sky-50 to-slate-100 flex flex-col overflow-hidden">
      <TrialBanner daysLeft={daysLeft} onUpgrade={handleUpgrade}/>
      {error&&<div className="flex-shrink-0 bg-red-50 border-b border-red-200 px-4 py-2 text-xs text-red-600">Error: {error}</div>}
      {gridHealth?.scattered&&<div className="flex-shrink-0 bg-amber-50 border-b border-amber-200 px-4 py-2 text-xs text-amber-800">Backend returning scattered points. See console.</div>}
      <div className="flex-1 flex overflow-hidden">
        <aside className={`${isMapExpanded?"hidden":"w-96 xl:w-[420px] flex-shrink-0"} flex flex-col border-r border-slate-200 bg-white overflow-hidden`}>
          <div className="p-3 border-b border-slate-200 flex items-center justify-between">
            <div className="text-xs font-semibold text-slate-600">Weather Powered By OceanCast</div>
            <UserMenu onUpgrade={handleUpgrade}/>
          </div>
          <div className="flex-1 overflow-hidden"><iframe src="https://noaa-parse-current.base44.app" className="w-full h-full border-0" title="NOAA Weather Watch"/></div>
        </aside>
        <main className="flex-1 flex flex-col overflow-hidden">
          {loading?(
            <div className="flex-1 flex items-center justify-center"><div className="flex flex-col items-center gap-3"><div className="w-10 h-10 border-4 border-slate-200 border-t-cyan-500 rounded-full animate-spin"/><p className="text-sm text-slate-500 font-medium">Loading SST data...</p></div></div>
          ):!activeGrid?.length?(
            <div className="flex-1 flex items-center justify-center"><div className="text-center text-slate-400 text-sm"><div className="text-2xl mb-2">🌊</div><div>No data available for this source yet.</div><div className="text-xs mt-1 text-slate-300">Try switching to a different SST source.</div></div></div>
          ):(
            <>
              <div className={`${isMapExpanded ? "flex-1 overflow-hidden" : "overflow-hidden p-2 sm:p-3 pb-0"} relative`}>
                <SSTHeatmapLeaflet
                  data={heatmapData} sstMin={sstMin} sstMax={sstMax}
                  date={selectedDate} dataSource={dataSource} setDataSource={setDataSource}
                  onLocationSaved={fetchSavedLocations} clearMarkersRef={clearMarkersRef} flyToRef={flyToRef}
                  onHoverSst={setLegendHoverSst} isMapExpanded={isMapExpanded} setIsMapExpanded={setIsMapExpanded}
                  activeDataLayer={activeDataLayer} setActiveDataLayer={setActiveDataLayer}
                  chlData={chlData} chlDateIndex={chlDateIndex} setChlDateIndex={setChlDateIndex} chlLoading={chlLoading}
                  seaColorData={seaColorData} seaColorDateIndex={seaColorDateIndex} setSeaColorDateIndex={setSeaColorDateIndex} seaColorLoading={seaColorLoading}
                  viirsData={viirsData} viirsDateIndex={viirsDateIndex} setViirsDateIndex={setViirsDateIndex} viirsHour={viirsHour} setViirsHour={setViirsHour}
                  viirsNppData={viirsNppData} viirsNppDateIndex={viirsNppDateIndex} setViirsNppDateIndex={setViirsNppDateIndex} activeViirsNppDay={activeViirsNppDay}
                  murData={murData} murDateIndex={murDateIndex} setMurDateIndex={setMurDateIndex}
                  goesCompData={goesCompData} goesCompDateIndex={goesCompDateIndex} setGoesCompDateIndex={setGoesCompDateIndex} activeGoesCompDay={activeGoesCompDay}
                  highlightedLocation={highlightedLocation} setHighlightedLocation={setHighlightedLocation}
                  savedLocations={savedLocations} fetchSavedLocations={fetchSavedLocations}
                  regionConfig={regionConfig}
                  selectedLocation={selectedLocation} setSelectedLocation={setSelectedLocation}
                  windData={windData} windLoading={windLoading}
                  windHourIndex={windHourIndex} setWindHourIndex={setWindHourIndex}
                  showWindOverlay={showWindOverlay} setShowWindOverlay={setShowWindOverlay}
                  windPlaying={windPlaying} setWindPlaying={setWindPlaying}
                />
              </div>
              <div className="flex-shrink-0 px-2 sm:px-3 pt-2 pb-2 sm:pb-3">
                {isWindMap
                  ? <WindLegend maxSpeed={windData?.maxSpeed ?? 30} />
                  : <SSTLegend sstMin={sstMin} sstMax={sstMax} hoverSst={legendHoverSst}/>
                }
              </div>
            </>
          )}
        </main>
        <aside className={`${isMapExpanded?"hidden":"w-56 xl:w-64"} flex-shrink-0 flex flex-col border-l border-slate-200 bg-white/60 overflow-y-auto`}>
          <div className="p-3 border-b border-slate-200"><div className="text-xs font-semibold text-slate-600">Saved Locations</div></div>
          <div className="flex-1 overflow-y-auto p-2">
            <SavedLocations locations={savedLocations} onRefresh={fetchSavedLocations} onClearMarkers={id=>clearMarkersRef.current?.(id)} onSelectLocation={(idx,loc)=>{if(!loc){setHighlightedLocation(null);return;}flyToRef.current?.(loc.lat,loc.lon);setHighlightedLocation(loc);}} highlightedId={highlightedLocation?.id}/>
          </div>
        </aside>
      </div>
    </div>
  );
}

export default function SSTLive() {
  function handleUpgrade(){alert("Upgrade coming soon!");}
  return(
    <AuthGate>
      <RegionGate region="outer_banks" onUpgrade={handleUpgrade}>
        <SSTPage/>
      </RegionGate>
    </AuthGate>
  );
}
