use std::fmt::Write as _;

use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars, tool, tool_handler, tool_router,
};
use sqlx::PgPool;
use tracing::error;

use crate::routes::search::{SearchRequest, execute_search};

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct FindSegmentsParams {
    /// Latitude of the search center point (-90 to 90)
    pub lat: f64,
    /// Longitude of the search center point (-180 to 180)
    pub lng: f64,
    /// Search radius in meters. Recommended: 3000–8000. Maximum: 50000.
    pub radius_m: f64,
    /// Target sustained power output in watts (must be > 0). For zone-based
    /// training pass the midpoint of the zone (e.g. 268W for Z4 at FTP 290W).
    pub power_w: f64,
    /// Target interval duration in seconds (e.g. 600 for 10 min). Practical
    /// range: 120–3600. The tool estimates which segments can be ridden at
    /// power_w for approximately this duration.
    pub interval_s: f64,
    /// Rider weight in kg for the physics model. Always pass the actual rider
    /// weight for accurate estimates — do not rely on the default of 78.
    pub weight_kg: Option<f64>,
    /// Maximum number of results to return (defaults to 5, max 20).
    pub limit: Option<usize>,
}

#[derive(Clone, Copy)]
pub struct KnownLocation {
    pub name: &'static str,
    pub lat: f64,
    pub lng: f64,
    pub description: &'static str,
}

pub const KNOWN_LOCATIONS: &[KnownLocation] = &[
    KnownLocation {
        name: "Avala",
        lat: 44.6913,
        lng: 20.5145,
        description: "Paved road, ~200m climb + rolling hills to the summit. Good for Z3-Z4 structured intervals, repeatable laps.",
    },
    KnownLocation {
        name: "Kosutnjak",
        lat: 44.7450,
        lng: 20.4225,
        description: "Light MTB trails, variable terrain, natural VO2max efforts on short punchy climbs. Gravel skills training.",
    },
    KnownLocation {
        name: "Topcider",
        lat: 44.7700,
        lng: 20.4400,
        description: "Park roads, short punchy climbs, social ride territory.",
    },
];

const MAX_RADIUS_M: f64 = 50_000.0;

#[derive(Clone)]
pub struct IntfndServer {
    pool: PgPool,
    #[expect(dead_code, reason = "held for rmcp #[tool_router] macro; used by rmcp at runtime")]
    tool_router: ToolRouter<IntfndServer>,
}

#[tool_router]
impl IntfndServer {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Find Strava segments matching a target power output and duration. \
        Segments are ranked by a blend of time accuracy (how closely the estimated ride time \
        at constant power matches the target interval) and popularity (Strava star count). \
        Provide lat/lng coordinates, or use list_known_locations to get coordinates for named \
        local places. Call list_known_locations first — do not guess coordinates for named locations.")]
    async fn find_segments(
        &self,
        Parameters(params): Parameters<FindSegmentsParams>,
    ) -> Result<CallToolResult, McpError> {
        if !(-90.0..=90.0).contains(&params.lat) || !(-180.0..=180.0).contains(&params.lng) {
            return Err(invalid_params("lat must be -90..90, lng must be -180..180"));
        }
        if params.power_w <= 0.0 || !params.power_w.is_finite() {
            return Err(invalid_params("power_w must be a positive finite number"));
        }
        if params.interval_s <= 0.0 || !params.interval_s.is_finite() {
            return Err(invalid_params("interval_s must be a positive finite number"));
        }
        if let Some(w) = params.weight_kg {
            if w <= 0.0 || !w.is_finite() {
                return Err(invalid_params("weight_kg must be a positive finite number"));
            }
        }

        if !params.radius_m.is_finite() || params.radius_m <= 0.0 {
            return Err(invalid_params("radius_m must be a positive finite number"));
        }

        let weight_kg = params.weight_kg.unwrap_or(78.0);
        let limit = params.limit.unwrap_or(5).clamp(1, 20);
        let radius_m = params.radius_m.min(MAX_RADIUS_M);

        let req = SearchRequest {
            lat: params.lat,
            lng: params.lng,
            radius_m,
            weight_kg,
            power_w: params.power_w,
            interval_s: params.interval_s,
        };

        let mut results = execute_search(&self.pool, &req).await.map_err(|e| {
            error!("MCP find_segments DB error: {e}");
            internal_error("Search failed")
        })?;

        results.truncate(limit);

        if results.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "No segments found matching the criteria. Try increasing radius_m or adjusting power_w/interval_s.",
            )]));
        }

        let mut output = format!(
            "Found {} segment(s) near ({:.4}, {:.4}) for {:.0}W / {:.0}s / {:.0}kg:\n\n",
            results.len(),
            params.lat,
            params.lng,
            params.power_w,
            params.interval_s,
            weight_kg,
        );
        for (i, r) in results.iter().enumerate() {
            let delta_abs = r.delta_s.abs();
            let direction = if r.delta_s >= 0.0 { "longer" } else { "shorter" };
            write!(
                output,
                "{}. {} — {:.1}km, {:.1}% grade, est. {:.0}s at {:.0}W ({:.0}s {} than target), {} stars\n   https://www.strava.com/segments/{}\n",
                i + 1,
                r.name,
                r.distance_m / 1000.0,
                r.average_grade,
                r.estimated_time_s,
                params.power_w,
                delta_abs,
                direction,
                r.star_count,
                r.strava_id,
            )
            .unwrap();
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "List known cycling locations near Belgrade with their coordinates. \
        Use the returned lat/lng as the center point for find_segments. \
        Always call this tool instead of guessing coordinates for a named location.")]
    fn list_known_locations(&self) -> Result<CallToolResult, McpError> {
        let mut output = String::from("Known cycling locations near Belgrade:\n\n");
        for loc in KNOWN_LOCATIONS {
            write!(
                output,
                "- {} (lat: {}, lng: {})\n  {}\n",
                loc.name, loc.lat, loc.lng, loc.description,
            )
            .unwrap();
        }
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }
}

#[tool_handler]
impl ServerHandler for IntfndServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder().enable_tools().build(),
        )
        .with_server_info(Implementation::from_build_env())
        .with_protocol_version(ProtocolVersion::LATEST)
        .with_instructions(
            "Finds Strava segments that match a target power output and duration. \
             Use list_known_locations to get coordinates for named places near Belgrade, \
             then find_segments to search for matching segments."
                .to_string(),
        )
    }
}

fn internal_error(msg: &'static str) -> McpError {
    McpError {
        code: ErrorCode(-32603),
        message: std::borrow::Cow::Borrowed(msg),
        data: None,
    }
}

fn invalid_params(msg: &'static str) -> McpError {
    McpError {
        code: ErrorCode(-32602),
        message: std::borrow::Cow::Borrowed(msg),
        data: None,
    }
}
