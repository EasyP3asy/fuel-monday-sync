require("dotenv").config();

const dns = require("dns");
dns.setDefaultResultOrder("ipv4first");

const fetch = require("node-fetch");
const cron = require("node-cron");

const mondayQueries = require("./monday-queries");
const motiveQueries = require("./motive-queries");

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.TELEGRAM_USER_ID;

const MONDAY_API_TOKEN = process.env.MONDAY_API_TOKEN;
const MONDAY_BASE_URL = process.env.MONDAY_BASE_URL;

const MONITORING_BOARD_ID = process.env.MONITORING_BOARD_ID;

const MOTIVE_BASE_URL = process.env.MOTIVE_BASE_URL;
const MOTIVE_X_API_KEY = process.env.MOTIVE_X_API_KEY;
const MOTIVE_X_WEB_USER_AUTH = process.env.MOTIVE_X_WEB_USER_AUTH;

const MOTIVE_FILTERED_GROUP_ID = process.env.MOTIVE_FILTERED_GROUP_ID;
const MOTIVE_TRUCKID_COLUMN_ID = process.env.MOTIVE_TRUCKID_COLUMN_ID;

const {
  MONITORING_BOARD_NAME_COL_ID,
  MONITORING_BOARD_TRUCKID_COL_ID,
  MONITORING_BOARD_TRUCK_NUMBER_COL_ID,
  MONITORING_BOARD_DASHCAM_STATUS_COL_ID,
  MONITORING_BOARD_DEF_PRCNT_COL_ID,
  MONITORING_BOARD_FUEL_PRCNT_COL_ID,
  MONITORING_BOARD_CAM_LAST_CAPTURE_COL_ID,
  MONITORING_BOARD_LOCATION_COL_ID,
  MONITORING_BOARD_LOCATION_UPDATE_DATE_COL_ID,
  MONITORING_BOARD_SPEED_COL_ID,
  MONITORING_BOARD_CURRENT_STATE_COL_ID,
  MONITORING_BOARD_STATE_SINCE_COL_ID,
  MONITORING_BOARD_SEAT_BELT_COL_ID,
  MONITORING_BOARD_TELEMATICS_UPDATE_DATE_COL_ID,
} = process.env;

const BATCH_SIZE = 1;
const BATCH_CONCURRENCY = 3;
const MOTIVE_FOLLOW_CONCURRENCY = 3;

main().catch((err) => {
  console.error("Startup run failed:", err);
});

async function main() {

  const mondayTrBoardContentsResponse = await makeApiRequest({
    url: MONDAY_BASE_URL,
    method: "POST",
    body: {
      query: mondayQueries.getColumnValuesFilterGroupAndColumn(
        MONITORING_BOARD_ID,
        MOTIVE_FILTERED_GROUP_ID,
        MOTIVE_TRUCKID_COLUMN_ID
      ),
    },
    token: MONDAY_API_TOKEN,
  });

  if (!mondayTrBoardContentsResponse?.data?.boards?.[0]?.items_page?.items) {
    throw new Error(
      "Failed to load existing monday board items. Aborting to avoid duplicate row creation."
    );
  }

  const motiveTruckInfosResponse = await makeApiRequest({
    url: `${MOTIVE_BASE_URL}v1/vehicles?per_page=100&page_no=1`,
    method: "GET",
    headers: {
      "x-api-key": MOTIVE_X_API_KEY,
    },
  });

  const boardTruckNumberIDArray = mondayTrBoardContentsResponse?.data?.boards?.[0]?.items_page?.items || [];

  const rowIdToTruckIdMap = new Map();
  const truckIdToRowIdMap = new Map();

  if (Array.isArray(boardTruckNumberIDArray) && boardTruckNumberIDArray.length > 0) {
    for (const item of boardTruckNumberIDArray) {
      const truckId = item?.column_values?.[0]?.text ?? null;
      const rowId = item?.id;

      if (rowId) {
        rowIdToTruckIdMap.set(rowId, truckId);
      }

      if (truckId) {
        truckIdToRowIdMap.set(String(truckId), rowId);
      }
    }
  }

  const motiveTruckInfoArray = motiveTruckInfosResponse?.vehicles || [];
  if (!Array.isArray(motiveTruckInfoArray) || motiveTruckInfoArray.length === 0) {
    console.warn("No vehicles returned from Motive.");
    return;
  }

  const motiveTruckInfoQueriesArray = [];

  for (const truck of motiveTruckInfoArray) {
    const truckId = truck?.vehicle?.id;
    if (truckId) {
      motiveTruckInfoQueriesArray.push(motiveQueries.getCurrentTruckInfo(truckId));
    }
  }

  const truckInfoResults = await fetchWithLimit(
    motiveTruckInfoQueriesArray,
    MOTIVE_FOLLOW_CONCURRENCY
  );

  const truckIdToTruckDataMap = new Map();

  for (const result of truckInfoResults) {
    const truckInfo = result?.travel_group;

    if (!truckInfo?.vehicle?.id) {
      continue;
    }

    const truckStatus = truckInfo?.vehicle?.status;
    if (truckStatus === "deactivated") {
      continue;
    }

    const truckId = truckInfo?.vehicle?.id;
    const truckNumber = truckInfo?.vehicle?.number ?? "N/A";

    const driverFirstName = truckInfo?.driver?.first_name || "";
    const driverLastName = truckInfo?.driver?.last_name || "";

    let dashCamStatus =
      truckInfo?.vehicle?.dashcam_status === "camera_obstructed"
        ? "Obstructed"
        : truckInfo?.vehicle?.dashcam_status || "N/A";

    const cameraLastCaptureRaw =
      truckInfo?.vehicle?.image_check?.last_image_metadata?.image_received_time || null;

    if (dashCamStatus !== "N/A") {
      const lastCapturedImageInMinutes = diffInMinutes(cameraLastCaptureRaw);
      if (
        typeof lastCapturedImageInMinutes === "number" &&
        lastCapturedImageInMinutes > 240
      ) {
        dashCamStatus = "Freezed";
      }
    }

    const latitude = truckInfo?.current_location?.lat ?? null;
    const longitude = truckInfo?.current_location?.lon ?? null;
    const formattedAddress = truckInfo?.current_location?.formatted_address || "N/A";

    const locationUpdatedRaw = truckInfo?.current_location?.located_at || null;
    const currentState = truckInfo?.current_state?.entity_state || "N/A";
    const currentSpeed = truckInfo?.current_location?.ground_speed_kph ?? "N/A";
    const currentStateSinceRaw =
      truckInfo?.current_state?.entity_state_last_updated || null;

    const currentTotalIdleSeconds =
      truckInfo?.current_state?.total_idle_seconds ?? 0;

    const driverSeatBeltStatus =
      truckInfo?.telematics_state?.driver_seat_belt_status != null
        ? String(truckInfo.telematics_state.driver_seat_belt_status)
        : "N/A";

    const defPrcnt =
      truckInfo?.telematics_state?.def_level_percent ??
      null;

    const fuelPrcnt =
      truckInfo?.telematics_state?.fuel_level_percent ?? null;

    const telematicsLastUpdateRaw =
      truckInfo?.telematics_state?.max_last_updated_at || null;

    const truckInfoObject = {
      truckId,
      truckNumber,
      driverFirstName,
      driverLastName,
      dashCamStatus,
      cameraLastCaptureDate: diffToText(cameraLastCaptureRaw),
      latitude,
      longitude,
      formattedAddress,
      locationUpdatedDate: diffToText(locationUpdatedRaw),
      currentState,
      currentSpeed,
      currentStateSince: diffToText(currentStateSinceRaw),
      currentTotalIdleSeconds,
      driverSeatBeltStatus,
      defPrcnt,
      fuelPrcnt,
      telematicsLastUpdateDate: diffToText(telematicsLastUpdateRaw),
    };

    truckIdToTruckDataMap.set(String(truckId), truckInfoObject);
  }

  const ops = [];

  for (const [truckId, truckInfo] of truckIdToTruckDataMap) {
    const driverFullName =
      [truckInfo.driverFirstName, truckInfo.driverLastName]
        .filter(Boolean)
        .join(" ")
        .trim() || "N/A";

    const hasCoordinates =
      truckInfo.latitude != null && truckInfo.longitude != null;

    const colValues = {
      [MONITORING_BOARD_LOCATION_COL_ID]: hasCoordinates
        ? {
            url: `https://www.google.com/maps/search/?api=1&query=${truckInfo.latitude},${truckInfo.longitude}`,
            text: truckInfo.formattedAddress || "N/A",
          }
        : {
            url: "",
            text: truckInfo.formattedAddress || "N/A",
          },

      [MONITORING_BOARD_TRUCKID_COL_ID]: String(truckInfo.truckId ?? ""),
      [MONITORING_BOARD_TRUCK_NUMBER_COL_ID]: String(truckInfo.truckNumber ?? ""),
      [MONITORING_BOARD_DASHCAM_STATUS_COL_ID]: String(truckInfo.dashCamStatus ?? "N/A"),
      [MONITORING_BOARD_DEF_PRCNT_COL_ID]:
        truckInfo.defPrcnt != null ? String(truckInfo.defPrcnt) : "",
      [MONITORING_BOARD_FUEL_PRCNT_COL_ID]:
        truckInfo.fuelPrcnt != null ? String(truckInfo.fuelPrcnt) : "",
      [MONITORING_BOARD_CAM_LAST_CAPTURE_COL_ID]: String(
        truckInfo.cameraLastCaptureDate ?? "N/A"
      ),
      [MONITORING_BOARD_LOCATION_UPDATE_DATE_COL_ID]: String(
        truckInfo.locationUpdatedDate ?? "N/A"
      ),
      [MONITORING_BOARD_CURRENT_STATE_COL_ID]: String(truckInfo.currentState ?? "N/A"),
      [MONITORING_BOARD_SPEED_COL_ID]: String(truckInfo.currentSpeed ?? "N/A"),
      [MONITORING_BOARD_STATE_SINCE_COL_ID]: String(
        truckInfo.currentStateSince ?? "N/A"
      ),
      [MONITORING_BOARD_SEAT_BELT_COL_ID]: String(
        truckInfo.driverSeatBeltStatus ?? "N/A"
      ),
      [MONITORING_BOARD_TELEMATICS_UPDATE_DATE_COL_ID]: String(
        truckInfo.telematicsLastUpdateDate ?? "N/A"
      ),
    };

    


    const rowId = truckIdToRowIdMap.get(String(truckId));

    if (rowId) {
      colValues[MONITORING_BOARD_NAME_COL_ID] = driverFullName;
      ops.push({
        query: mondayQueries.updateMultipleAlliasColumnValuesQuery(
          MONITORING_BOARD_ID,
          rowId,
          colValues
        ), 
         meta: {
          truckId,
          rowId,
          driverFullName,
          defPrcnt: truckInfo.defPrcnt,
          fuelPrcnt: truckInfo.fuelPrcnt,
          currentSpeed: truckInfo.currentSpeed,
          currentState: truckInfo.currentState,
          mode: "update",
        },
    });
    } else {
      ops.push({
        query : mondayQueries.createMultipleAlliasColumnValuesQuery(
          ops.length,
          MONITORING_BOARD_ID,
          MOTIVE_FILTERED_GROUP_ID,
          driverFullName,
          colValues
        ),
         meta: {
          truckId,
          rowId: null,
          driverFullName,
          defPrcnt: truckInfo.defPrcnt,
          fuelPrcnt: truckInfo.fuelPrcnt,
          currentSpeed: truckInfo.currentSpeed,
          currentState: truckInfo.currentState,
          mode: "create",
        },

    });
    }
  }

  const updateBatches = chunk(ops, BATCH_SIZE).map((batch) => ({
    query: buildAliasedMutation(batch.map((x) => x.query)),
    meta: batch.map((x) => x.meta),
  }));

  if (updateBatches.length) {
    await runBatches(updateBatches);
  }

  console.log(`Sync complete. Processed ${truckIdToTruckDataMap.size} trucks.`);
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

function buildAliasedMutation(ops) {
  return `mutation {\n${ops.join("\n")}\n}`;
}

async function runBatches(batchQueries, concurrency = BATCH_CONCURRENCY) {
  let i = 0;

  const workers = new Array(Math.min(concurrency, batchQueries.length))
    .fill(0)
    .map(async () => {
      while (i < batchQueries.length) {
        const idx = i++;

        const result = await makeApiRequest({
          url: MONDAY_BASE_URL,
          method: "POST",
          token: MONDAY_API_TOKEN,
          body: {
            query: batchQueries[idx].query,
          },
        });

        if (result?.errors?.length) {
          console.warn(`Request ${idx + 1}/${batchQueries.length} failed with GraphQL errors`);
          console.dir(result.errors, { depth: null });
          console.dir(batchQueries[idx].meta, { depth: null });
        }


        if (result === null) {
          console.warn(`Request ${idx + 1}/${batchQueries.length} failed with no response after retries`);
          console.dir(batchQueries[idx].meta, { depth: null });
        }


      }
    });

  await Promise.all(workers);
}

function diffToText(earlier, later = new Date()) {
  if (!earlier) return "N/A";

  const start = earlier instanceof Date ? earlier : new Date(earlier);
  const end = later instanceof Date ? later : new Date(later);

  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    return "N/A";
  }

  let diffMs = end - start;
  if (diffMs < 0) diffMs = Math.abs(diffMs);

  const totalMinutes = Math.floor(diffMs / (1000 * 60));
  const days = Math.floor(totalMinutes / (60 * 24));
  const hours = Math.floor((totalMinutes % (60 * 24)) / 60);
  const mins = totalMinutes % 60;

  const parts = [];
  if (days) parts.push(`${days} day${days !== 1 ? "s" : ""}`);
  if (hours) parts.push(`${hours} hour${hours !== 1 ? "s" : ""}`);
  if (mins || parts.length === 0) {
    parts.push(`${mins} minute${mins !== 1 ? "s" : ""}`);
  }

  return `${parts.join(" ")} ago`;
}

function diffInMinutes(startDate, endDate = new Date()) {
  if (!startDate) return null;

  const dateA = startDate instanceof Date ? startDate : new Date(startDate);
  const dateB = endDate instanceof Date ? endDate : new Date(endDate);

  if (Number.isNaN(dateA.getTime()) || Number.isNaN(dateB.getTime())) {
    return null;
  }

  return (dateB.getTime() - dateA.getTime()) / (1000 * 60);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableStatus(status) {
  return [500, 502, 503, 504].includes(status);
}

function redactHeaders(headers = {}) {
  const safeHeaders = { ...headers };
  for (const key of ["Authorization", "x-api-key", "x-web-user-auth"]) {
    if (safeHeaders[key]) {
      safeHeaders[key] = "[REDACTED]";
    }
  }
  return safeHeaders;
}

function hasRetryableMondayGraphQLError(responseData) {
  if (!responseData || !Array.isArray(responseData.errors)) return false;

  const text = JSON.stringify(responseData.errors).toLowerCase();

  return (
    text.includes("timeout") ||
    text.includes("internal server error") ||
    text.includes("bad gateway") ||
    text.includes("gateway timeout") ||
    text.includes("temporar") ||
    text.includes("exception")
  );
}

async function makeApiRequest({
  url,
  method = "GET",
  body,
  headers = {},
  token,
  queryParams,
  retries = 4,
  retryDelayMs = 1000,
}) {
  let finalUrl = url;

  if (queryParams && typeof queryParams === "object") {
    const qs = new URLSearchParams(queryParams).toString();
    if (qs) {
      finalUrl += (finalUrl.includes("?") ? "&" : "?") + qs;
    }
  }

  const finalHeaders = { ...headers };
  if (token) {
    finalHeaders.Authorization = token;
  }

  const options = {
    method: method.toUpperCase(),
    headers: finalHeaders,
  };

  if (body !== undefined && options.method !== "GET" && options.method !== "HEAD") {
    const isSpecialBody =
      body instanceof FormData ||
      body instanceof URLSearchParams ||
      typeof body === "string" ||
      body instanceof Blob ||
      body instanceof ArrayBuffer;

    if (isSpecialBody) {
      options.body = body;
    } else {
      options.body = JSON.stringify(body);
      if (!finalHeaders["Content-Type"] && !finalHeaders["content-type"]) {
        finalHeaders["Content-Type"] = "application/json";
      }
    }
  }

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await fetch(finalUrl, options);
      const contentType = response.headers.get("content-type") || "";

      let responseData;
      if (contentType.includes("application/json")) {
        responseData = await response.json();
      } else {
        responseData = await response.text();
      }

      if (!response.ok) {
        console.error("API request failed:", {
          url: finalUrl,
          method: options.method,
          status: response.status,
          statusText: response.statusText,
          headers: redactHeaders(finalHeaders),
          responseData,
          attempt,
        });

        if (attempt < retries && isRetryableStatus(response.status)) {
          await sleep(retryDelayMs * Math.pow(2, attempt - 1));
          continue;
        }

        return null;
      }

      if (
        finalUrl.includes("monday.com") &&
        responseData &&
        Array.isArray(responseData.errors)
      ) {
       

        console.error("Monday GraphQL error:");
        console.dir(responseData, { depth: null });
        

        if (attempt < retries && hasRetryableMondayGraphQLError(responseData)) {
          await sleep(retryDelayMs * Math.pow(2, attempt - 1));
          continue;
        }

        return responseData;
      }

      return responseData;
    } catch (error) {
      console.error("Error in API request:", {
        url: finalUrl,
        method: options.method,
        headers: redactHeaders(finalHeaders),
        error: error instanceof Error ? error.message : String(error),
        attempt,
      });

      if (attempt < retries) {
        await sleep(retryDelayMs * Math.pow(2, attempt - 1));
        continue;
      }

      return null;
    }
  }

  return null;
}

function escapeMarkdown(text = "") {
  return String(text).replace(/([_*[\]()~`>#+\-=|{}.!\\])/g, "\\$1");
}

async function sendErrorToTelegram(messageText) {
  if (!TELEGRAM_BOT_TOKEN || !CHAT_ID) {
    console.warn("Telegram credentials are missing. Skipping Telegram alert.");
    return;
  }

  const message = `*Motive Monitor Error* 🚨\n\n${escapeMarkdown(messageText)}`;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  const params = {
    chat_id: CHAT_ID,
    text: message,
    parse_mode: "MarkdownV2",
  };

  try {
    const telegramRes = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });

    if (!telegramRes.ok) {
      const errorText = await telegramRes.text();
      throw new Error(`Telegram API error: ${errorText}`);
    }

    console.log("Telegram alert sent");
  } catch (err) {
    console.error("Failed to send Telegram message:", err);
  }
}

let isRunning = false;

cron.schedule(
  "*/15 * * * *",
  async () => {
    if (isRunning) {
      console.log("Cron: previous run still in progress, skipping this one");
      return;
    }

    isRunning = true;
    console.log("Cron: starting run at", new Date().toISOString());

    try {
      await main();
      console.log("Cron: finished run");
    } catch (err) {
      console.error("Cron: error in run:", err);
      await sendErrorToTelegram(
        err instanceof Error ? err.message : String(err)
      );
    } finally {
      isRunning = false;
    }
  },
  {
    timezone: "America/New_York",
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
          method: "GET",
          headers: {
            "x-web-user-auth": MOTIVE_X_WEB_USER_AUTH,
          },
        });

        if (data === null) {
          results[current] = {
            url,
            error: "Request failed (makeApiRequest returned null)",
          };
        } else {
          results[current] = data;
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