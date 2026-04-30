const G: f64 = 9.81;   // gravitational acceleration, m/s²
const CRR: f64 = 0.004; // rolling resistance coefficient (road tire on pavement)
const RHO: f64 = 1.225; // air density, kg/m³ (sea level, 15 °C)
const CDA: f64 = 0.32;  // Cd × frontal area, m² (road cyclist, hoods position)
const ETA: f64 = 0.95;  // drivetrain efficiency (5% loss in chain/bearings)

/// Returns the estimated ride time in seconds, or None for segments where
/// the model doesn't apply (zero/negative power, unconverged solution).
pub fn estimated_time(distance_m: f64, grade_pct: f64, weight_kg: f64, power_w: f64) -> Option<f64> {
    if power_w <= 0.0 || distance_m <= 0.0 {
        return None;
    }

    let grade = grade_pct / 100.0;
    let p_eff = power_w * ETA;

    // Three resisting forces:
    let f_gravity  = weight_kg * G * grade;          // positive = uphill, negative = downhill
    let f_rolling  = weight_kg * G * CRR;             // always opposes motion
    // f_aero = 0.5 * RHO * CDA * v²                 // computed per Newton iteration below

    let f_const = f_gravity + f_rolling;              // speed-independent component

    // Solve for v: 0.5·ρ·CdA·v³ + f_const·v − P_eff = 0  (Newton's method)
    // Initial guess ignores aero drag; clamp to avoid zero
    let v_init = if f_const > 0.0 { p_eff / f_const } else { 5.0 };
    let mut v = v_init.clamp(0.1, 30.0);

    for _ in 0..64 {
        let f_aero = 0.5 * RHO * CDA * v * v;
        let fv  = (f_const + f_aero) * v - p_eff;    // residual
        let dfv = f_const + 3.0 * 0.5 * RHO * CDA * v * v; // derivative
        if dfv.abs() < 1e-12 {
            break;
        }
        let delta = fv / dfv;
        v = (v - delta).max(0.01);
        if delta.abs() < 1e-9 {
            break;
        }
    }

    // Sanity: verify convergence by checking residual
    let f_aero_final = 0.5 * RHO * CDA * v * v;
    let residual = ((f_const + f_aero_final) * v - p_eff).abs();
    if residual > 1.0 {
        return None;
    }

    Some(distance_m / v)
}
