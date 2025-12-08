require('dotenv').config();

const dns = require("dns");
dns.setDefaultResultOrder("ipv4first");

const fetch = require('node-fetch');


const mondayQueries = require('./monday-queries');
const motiveQueries = require('./motive-queries');
const cron = require("node-cron");






const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.TELEGRAM_USER_ID;

const MONDAY_API_TOKEN = process.env.MONDAY_API_TOKEN;
const MONDAY_BASE_URL = process.env.MONDAY_BASE_URL;

const MONITORING_BOARD_ID =  process.env.MONITORING_BOARD_ID;

const MOTIVE_BASE_URL = process.env.MOTIVE_BASE_URL;
const MOTIVE_X_API_KEY = process.env.MOTIVE_X_API_KEY;

const MOTIVE_X_WEB_USER_AUTH = process.env.MOTIVE_X_WEB_USER_AUTH;
const MOTIVE_FILTERED_GROUP_ID = process.env.MOTIVE_FILTERED_GROUP_ID;
const MOTIVE_TRUCKID_COLUMN_ID =  process.env.MOTIVE_TRUCKID_COLUMN_ID;

const {
  MONITORING_BOARD_NAME_COL_ID ,
  MONITORING_BOARD_TRUCKID_COL_ID, 
  MONITORING_BOARD_TRUCK_NUMBER_COL_ID,   
  MONITORING_BOARD_DASHCAM_STATUS_COL_ID, 
  MONITORING_BOARD_DEF_PRCNT_COL_ID ,
  MONITORING_BOARD_FUEL_PRCNT_COL_ID ,
  MONITORING_BOARD_CAM_LAST_CAPTURE_COL_ID,             
  MONITORING_BOARD_LOCATION_COL_ID,
  MONITORING_BOARD_LOCATION_UPDATE_DATE_COL_ID, 
  MONITORING_BOARD_SPEED_COL_ID ,
  MONITORING_BOARD_CURRENT_STATE_COL_ID,
  MONITORING_BOARD_STATE_SINCE_COL_ID, 
  MONITORING_BOARD_SEAT_BELT_COL_ID ,
  MONITORING_BOARD_TELEMATICS_UPDATE_DATE_COL_ID 
} = process.env;


const BATCH_SIZE = 20;        // << group into 20 ops
const BATCH_CONCURRENCY = 3;  // run up to 3 batches in parallel (tweak if needed)





main();

async function main() {

    // fetching trailer board truck numbers and row IDs 
    const mondayTrBoardContentsResponse = await makeApiRequest({      
        url:MONDAY_BASE_URL,
        method:'POST',
        body:{
            query: await mondayQueries.getColumnValuesFilterGroupAndColumn(MONITORING_BOARD_ID,MOTIVE_FILTERED_GROUP_ID,MOTIVE_TRUCKID_COLUMN_ID),
        },        
        token : MONDAY_API_TOKEN
    });

    const motiveTruckInfosResponse = await makeApiRequest({
        url: MOTIVE_BASE_URL+"v1/vehicles?per_page=100&page_no=1",           // fetches 100 truckInfos perPage
        method:'GET',
        headers : {
            'x-api-key': MOTIVE_X_API_KEY
        }
    });

    const boardTruckNumberIDArray = mondayTrBoardContentsResponse?.data?.boards?.[0]?.items_page?.items;
    const rowIdToTruckIdMap = new Map();
    const truckIdToRowIdMap = new Map();

    if(Array.isArray(boardTruckNumberIDArray) && boardTruckNumberIDArray.length>0){

      for(const item of boardTruckNumberIDArray) {
        const truckId = item.column_values?.[0]?.text ?? null; 
        const rowId = item?.id;
        rowIdToTruckIdMap.set(rowId, truckId);
        if(truckId){
          truckIdToRowIdMap.set(String(truckId),rowId);
        }
      }

    }
    
    
   

    const motiveTruckInfoArray = motiveTruckInfosResponse?.vehicles;   

    const motiveTruckInfoQueriesArray = [];
    const unknownTruckIdNumberObjectArray =[];

    for(const truck of motiveTruckInfoArray){           // setting truck Number to Truck ID 
        const truckId = truck?.vehicle?.id;
        const truckNumber = String(truck?.vehicle?.number);        
        if(truckId){
          motiveTruckInfoQueriesArray.push(motiveQueries.getCurrentTruckInfo(truckId));
        }
    }


   
   
   


    const truckInfoResults = await fetchWithLimit(motiveTruckInfoQueriesArray);
    const truckIdToTruckDataMap = new Map();


    
    for(let truckInfo of truckInfoResults){
      truckInfo = truckInfo.travel_group;
      const  truckStatus = truckInfo?.vehicle?.status;

      if(truckStatus == "deactivated"){
        continue;
      }

      const truckId = truckInfo?.vehicle?.id;
      const truckNumber = truckInfo?.vehicle?.number;
      const driverFirstName = truckInfo?.driver?.first_name;
      const driverLastName = truckInfo?.driver?.last_name;
      
      let dashCamStatus = truckInfo?.vehicle?.dashcam_status == "camera_obstructed" ? "Obstructed" : truckInfo?.vehicle?.dashcam_status;
      let cameraLastCaptureDate = truckInfo?.vehicle?.image_check?.last_image_metadata?.image_received_time || null;

      if(dashCamStatus){
        const lastCapturedImageInMinutes = diffInMinutes(cameraLastCaptureDate);        
        if(lastCapturedImageInMinutes && lastCapturedImageInMinutes > 240 ){
          dashCamStatus = "Freezed";
        }        
      }else{
          dashCamStatus= "N/A"
      }
      
      cameraLastCaptureDate = formatToEasternTime(cameraLastCaptureDate);
       

      
      
      
      const latitude = truckInfo?.current_location?.lat;
      const longitude = truckInfo?.current_location?.lon;
      const formattedAddress = truckInfo?.current_location?.formatted_address;
      let locationUpdatedDate = truckInfo?.current_location?.located_at || null;
      locationUpdatedDate = formatToEasternTime(locationUpdatedDate);

      const currentState = truckInfo?.current_state?.entity_state;
      const currentSpeed = truckInfo?.current_state?.ground_speed_kph;
      let currentStateSince = truckInfo?.current_state?.entity_state_last_updated || null;
      currentStateSince = formatToEasternTime(currentStateSince);
      const currentTotalIdleSeconds = truckInfo?.current_state?.total_idle_seconds;

      const driverSeatBeltStatus = truckInfo?.telematics_state?.driver_seat_belt_status;

      const defPrcnt = truckInfo?.telematics_state?.def_level_percent;
      const fuelPrcnt = truckInfo?.telematics_state?.fuel_level_percent;
      const odometer = truckInfo?.telematics_state?.odometer;
      const engineHours =truckInfo?.telematics_state?.engine_hours;
      let telematicsLastUpdateDate = truckInfo?.telematics_state?.max_last_updated_at || null;
      telematicsLastUpdateDate = formatToEasternTime(telematicsLastUpdateDate);
      
      
    


      const truckInfoObject ={
        truckId,
        truckNumber,
        driverFirstName,
        driverLastName,
        dashCamStatus,
        cameraLastCaptureDate:diffToText(cameraLastCaptureDate),
        latitude,
        longitude,
        formattedAddress,
        locationUpdatedDate : diffToText(locationUpdatedDate),
        currentState,
        currentSpeed,
        currentStateSince: diffToText(currentStateSince),
        currentTotalIdleSeconds,
        driverSeatBeltStatus,
        defPrcnt,
        fuelPrcnt,
        telematicsLastUpdateDate : diffToText(telematicsLastUpdateDate)
      }     
      truckIdToTruckDataMap.set(truckId,truckInfoObject);
       
    }   

    
    let ops = [];
    
    

    for(const [truckId, truckInfo] of truckIdToTruckDataMap){

          const driverFullName = `${truckInfo.driverFirstName} ${truckInfo.driverLastName}` || "N/A";
          
          const colValues ={
          [MONITORING_BOARD_LOCATION_COL_ID]:{                   // change link column 
            "url":`www.google.com/maps/search/?api=1&query=${truckInfo.latitude},${truckInfo.longitude}`,
            "text" :`${truckInfo.formattedAddress}`
          },
          
          [MONITORING_BOARD_TRUCKID_COL_ID]:`${truckInfo.truckId}`,     
          [MONITORING_BOARD_TRUCK_NUMBER_COL_ID] : `${truckInfo.truckNumber}`,       
          [MONITORING_BOARD_DASHCAM_STATUS_COL_ID] : `${truckInfo.dashCamStatus}`,
          [MONITORING_BOARD_DEF_PRCNT_COL_ID]: `${truckInfo.fuelPrcnt}`,
          [MONITORING_BOARD_FUEL_PRCNT_COL_ID]: `${truckInfo.defPrcnt}`,
          [MONITORING_BOARD_CAM_LAST_CAPTURE_COL_ID]:`${truckInfo.cameraLastCaptureDate}`,
          [MONITORING_BOARD_LOCATION_UPDATE_DATE_COL_ID]:`${truckInfo.locationUpdatedDate}`,
          [MONITORING_BOARD_CURRENT_STATE_COL_ID] : `${truckInfo.currentState}`,
          [MONITORING_BOARD_SPEED_COL_ID] : `${truckInfo.currentSpeed}`,
          [MONITORING_BOARD_STATE_SINCE_COL_ID] : `${truckInfo.currentStateSince}`,
          [MONITORING_BOARD_SEAT_BELT_COL_ID] :`${truckInfo.driverSeatBeltStatus}`,
          [MONITORING_BOARD_TELEMATICS_UPDATE_DATE_COL_ID] :`${truckInfo.telematicsLastUpdateDate}`         
        }
        const rowId = truckIdToRowIdMap.get(String(truckId));

        if(rowId){        
          colValues[MONITORING_BOARD_NAME_COL_ID] = driverFullName;
          ops.push(mondayQueries.updateMultipleAlliasColumnValuesQuery(MONITORING_BOARD_ID,rowId,colValues));                
            
        }else{
          ops.push(mondayQueries.createMultipleAlliasColumnValuesQuery(ops.length,MONITORING_BOARD_ID,MOTIVE_FILTERED_GROUP_ID,driverFullName,colValues));
        }
    }
    
  
    const updateBatches = chunk(ops, BATCH_SIZE).map(buildAliasedMutation);

   


    if (updateBatches.length) {
      await runBatches(updateBatches);
    }
    

}




function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}


function buildAliasedMutation(ops) {
  return `mutation{\n${ops.join('\n')}\n}`;
}


  

async function runBatches(batchQueries, concurrency = BATCH_CONCURRENCY) {
  let i = 0;
  const workers = new Array(Math.min(concurrency, batchQueries.length)).fill(0).map(async () => {
    while (i < batchQueries.length) {
      const idx = i++;
      await makeApiRequest({
          url:MONDAY_BASE_URL,
          method:'POST',
          token : MONDAY_API_TOKEN,
          body: {
            query:batchQueries[idx]
          } 
        } 
      );
    
    }
  });
  await Promise.all(workers);
}






function formatToEasternTime(input) { 
  if(!input){
    return 'N/A';
  }
  const date = new Date(input);
  
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    month: 'short',       // "May"
    day: 'numeric',      // "2"
    year: 'numeric',     // "2025"
    hour: 'numeric',     // "2"
    minute: '2-digit',   // "07"
    hour12: true,        // "PM"
    timeZoneName: 'short' // "EDT"
  }).format(date);
}



function diffToText(earlier, later = new Date()) {
  if(earlier =="N/A"){
    return "N/A"
  }

  const start = (earlier instanceof Date) ? earlier : new Date(earlier);
  const end   = (later   instanceof Date) ? later   : new Date(later);

  let diffMs = end - start;

  if (diffMs < 0) {
    // if earlier is actually in the future
    diffMs = -diffMs;
  }

  const totalMinutes = Math.floor(diffMs / (1000 * 60));
  const days   = Math.floor(totalMinutes / (60 * 24));
  const hours  = Math.floor((totalMinutes % (60 * 24)) / 60);
  const mins   = totalMinutes % 60;

  const parts = [];
  if (days)  parts.push(`${days} day${days !== 1 ? "s" : ""}`);
  if (hours) parts.push(`${hours} hour${hours !== 1 ? "s" : ""}`);
  if (mins || parts.length === 0) {
    parts.push(`${mins} minute${mins !== 1 ? "s" : ""}`);
  }

  return parts.join(" ") + " ago";
}


function diffInMinutes(startDate, endDate = new Date()) {
  const dateA = startDate instanceof Date ? startDate : new Date(startDate);
  const dateB = endDate instanceof Date ? endDate : new Date(endDate);

  // if either date is invalid, return null (or throw, your choice)
  if (Number.isNaN(dateA.getTime()) || Number.isNaN(dateB.getTime())) {
    return "N/A";
  }

  const diffMs = dateB.getTime() - dateA.getTime(); // ms difference
  const diffMinutes = diffMs / (1000 * 60);         // convert to minutes

  return diffMinutes; // can be negative if b < a
}



async function makeApiRequest({
  url,
  method = 'GET',
  body,                 // request body (object, string, FormData, etc.)
  headers = {},         // custom headers
  token,                // optional Bearer token
  queryParams,          // optional query-string params as object
}) {
  try {
    // --- Build final URL with query params if provided ---
    let finalUrl = url;
    if (queryParams && typeof queryParams === 'object') {
      const qs = new URLSearchParams(queryParams).toString();
      if (qs) {
        finalUrl += (finalUrl.includes('?') ? '&' : '?') + qs;
      }
    }
    
    
    // --- Build headers dynamically ---
    const finalHeaders = { ...headers };
    if (token) {
      finalHeaders.Authorization = `Bearer ${token}`;
    }

    const options = {
      method: method.toUpperCase(),
      headers: finalHeaders,
    };

    // --- Attach body only for non-GET/HEAD methods ---
    if (body !== undefined && options.method !== 'GET' && options.method !== 'HEAD') {
      // If body is FormData, URLSearchParams, string, Blob, etc. -> send as is
      const isSpecialBody =
        body instanceof FormData ||
        body instanceof URLSearchParams ||
        typeof body === 'string' ||
        body instanceof Blob ||
        body instanceof ArrayBuffer;

      if (isSpecialBody) {
        options.body = body;
      } else {
        // Assume plain object → send as JSON
        options.body = JSON.stringify(body);

        // Only set Content-Type if user didn’t already override it
        if (!finalHeaders['Content-Type'] && !finalHeaders['content-type']) {
          finalHeaders['Content-Type'] = 'application/json';
        }
      }
    }

    const response = await fetch(finalUrl, options);

    // Try to parse JSON if possible, otherwise return text
    const contentType = response.headers.get('content-type') || '';
    const responseData = contentType.includes('application/json')
      ? await response.json()
      : await response.text();

    if (!response.ok) {
      const safeHeaders = { ...finalHeaders };
      for (const k of ["Authorization","x-api-key","x-web-user-auth"]) {
        if (safeHeaders[k]) safeHeaders[k] = "[REDACTED]";
      }
      console.error("API request failed:", {
        url: finalUrl,
        method: options.method,
        status: response.status,
        statusText: response.statusText,
        headers: safeHeaders,
        responseData,
      });
      return null;
    }

    return responseData;
  } catch (error) {
    console.error('Error in API request:', error);
    return null;
  }
}






async function sendErrorToTelegram(messageText) {
  const message = `🚨 *Alert!* 🚨\n\n${escapeMarkdown(messageText)}`;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  const params = {
    chat_id: CHAT_ID,
    text: message,
    parse_mode: "Markdown"
  };

  try {
    const telegramRes = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params)
    });

    if (!telegramRes.ok) {
      const errorText = await telegramRes.text();
      throw new Error(`Telegram API error: ${errorText}`);
    }

    console.log("✅ Telegram alert sent");
  } catch (err) {
    console.error("❌ Failed to send Telegram message:", err);
  }
}







let isRunning = false; // to prevent overlap


cron.schedule(
  "*/15 * * * *",  // every 30 minutes
  async () => {
    if (isRunning) {
      console.log("Cron: previous run still in progress, skipping this one");
      return;
    }

    isRunning = true;
    console.log("Cron: starting runMotiveScrapping at", new Date().toISOString());

    try {
      await main();
      console.log("Cron: finished runMotiveScrapping");
    } catch (err) {
      console.error("Cron: error in runMotiveScrapping:", err);
    } finally {
      isRunning = false;
    }
  },
  {
    timezone: "America/New_York", // optional, but nice for your EST use case
  }
);



async function fetchWithLimit(urls, limit = 5) {
  const results = new Array(urls.length);
  let index = 0;

  async function worker() {
    while (true) {
      const current = index++;
      if (current >= urls.length) break;

      const url = urls[current];

      try {
        const data = await makeApiRequest({
          url,
          method: 'GET', // optional if your helper auto-chooses GET
          headers: {
            'x-web-user-auth': MOTIVE_X_WEB_USER_AUTH,
          },
        });

        if (data === null) {
          // makeApiRequest already logged the error
          results[current] = {
            url,
            error: 'Request failed (makeApiRequest returned null)',
          };
        } else {
          results[current] = data; // already parsed JSON / text
        }
      } catch (err) {
        results[current] = {
          url,
          error: err instanceof Error ? err.message : String(err),
        };
      }
    }
  }

  const workers = Array.from(
    { length: Math.min(limit, urls.length) },
    () => worker()
  );

  await Promise.all(workers);
  return results;
}
