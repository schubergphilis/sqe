//! Trino's distribution functions on plain `f64`: `normal_cdf`,
//! `inverse_normal_cdf`, `beta_cdf`, `inverse_beta_cdf`, `t_cdf`, `t_pdf`.
//!
//! The SQL wrappers live in [`crate::math_fns`]; this module holds the
//! numerics and Trino's argument checks. The error text follows Trino's
//! (`checkCondition` messages in `MathFunctions`).
//!
//! - The normal distribution goes through `erfc`, the way commons-math does.
//! - The regularized incomplete beta function uses the continued fraction
//!   evaluated with the modified Lentz method. `t_cdf` is a beta function of
//!   `df / (df + v^2)`.
//! - Both inverses refine a start value to full `f64` precision: the normal
//!   one with Halley steps, the beta one by bisection on the CDF.

use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;

fn invalid(msg: &str) -> DataFusionError {
    DataFusionError::Execution(msg.to_string())
}

/// `true` when `x` is a number strictly above zero (NaN is not).
fn positive(x: f64) -> bool {
    x > 0.0
}

/// `true` when `p` lies in `[0, 1]` (NaN does not).
fn is_probability(p: f64) -> bool {
    (0.0..=1.0).contains(&p)
}

pub(crate) fn normal_cdf(mean: f64, sd: f64, value: f64) -> DFResult<f64> {
    if !positive(sd) {
        return Err(invalid("standardDeviation must be > 0"));
    }
    let dev = value - mean;
    // Beyond 40 standard deviations the answer is 0 or 1 to double precision.
    if dev.abs() > 40.0 * sd {
        return Ok(if dev < 0.0 { 0.0 } else { 1.0 });
    }
    Ok(0.5 * libm::erfc(-dev / (sd * std::f64::consts::SQRT_2)))
}

pub(crate) fn inverse_normal_cdf(mean: f64, sd: f64, p: f64) -> DFResult<f64> {
    if !is_probability(p) {
        return Err(invalid("p must be 0 > p > 1"));
    }
    if !positive(sd) {
        return Err(invalid("sd must be > 0"));
    }
    Ok(mean + sd * standard_normal_quantile(p))
}

/// The `p` quantile of the standard normal: Acklam's rational approximation
/// (relative error 1e-9), then two Halley steps against `erfc`.
fn standard_normal_quantile(p: f64) -> f64 {
    if p == 0.0 {
        return f64::NEG_INFINITY;
    }
    if p == 1.0 {
        return f64::INFINITY;
    }
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    const P_LOW: f64 = 0.02425;
    let tail = |q: f64| {
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    };
    let mut x = if p < P_LOW {
        tail((-2.0 * p.ln()).sqrt())
    } else if p <= 1.0 - P_LOW {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        -tail((-2.0 * (-p).ln_1p()).sqrt())
    };
    for _ in 0..2 {
        let err = 0.5 * libm::erfc(-x / std::f64::consts::SQRT_2) - p;
        let u = err * (2.0 * std::f64::consts::PI).sqrt() * (x * x / 2.0).exp();
        x -= u / (1.0 + x * u / 2.0);
    }
    x
}

/// `ln(B(a, b))`.
fn ln_beta(a: f64, b: f64) -> f64 {
    libm::lgamma(a) + libm::lgamma(b) - libm::lgamma(a + b)
}

/// The continued fraction of the incomplete beta function, modified Lentz.
fn beta_continued_fraction(a: f64, b: f64, x: f64) -> f64 {
    const TINY: f64 = 1e-300;
    const MAX_ITERATIONS: u32 = 100_000;
    let nonzero = |v: f64| if v.abs() < TINY { TINY } else { v };
    let (qab, qap, qam) = (a + b, a + 1.0, a - 1.0);
    let mut c = 1.0;
    let mut d = 1.0 / nonzero(1.0 - qab * x / qap);
    let mut h = d;
    for m in 1..=MAX_ITERATIONS {
        let m = f64::from(m);
        let m2 = 2.0 * m;
        let even = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 / nonzero(1.0 + even * d);
        c = nonzero(1.0 + even / c);
        h *= d * c;
        let odd = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 / nonzero(1.0 + odd * d);
        c = nonzero(1.0 + odd / c);
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() < f64::EPSILON {
            break;
        }
    }
    h
}

/// The regularized incomplete beta function `I_x(a, b)` for `a, b > 0`.
fn regularized_beta(a: f64, b: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let front = (a * x.ln() + b * (-x).ln_1p() - ln_beta(a, b)).exp();
    // The fraction converges fast on the side of the mean it is built for.
    if x < (a + 1.0) / (a + b + 2.0) {
        front * beta_continued_fraction(a, b, x) / a
    } else {
        1.0 - front * beta_continued_fraction(b, a, 1.0 - x) / b
    }
}

fn check_beta_shape(a: f64, b: f64) -> DFResult<()> {
    if positive(a) && positive(b) {
        Ok(())
    } else {
        Err(invalid("a, b must be > 0"))
    }
}

pub(crate) fn beta_cdf(a: f64, b: f64, value: f64) -> DFResult<f64> {
    check_beta_shape(a, b)?;
    if !is_probability(value) {
        return Err(invalid("value must be in the interval [0, 1]"));
    }
    Ok(regularized_beta(a, b, value))
}

pub(crate) fn inverse_beta_cdf(a: f64, b: f64, p: f64) -> DFResult<f64> {
    check_beta_shape(a, b)?;
    if !is_probability(p) {
        return Err(invalid("p must be in the interval [0, 1]"));
    }
    if p == 0.0 || p == 1.0 {
        return Ok(p);
    }
    // I_x(a, b) rises from 0 to 1, so bisection always closes in.
    let (mut lo, mut hi) = (0.0_f64, 1.0_f64);
    for _ in 0..200 {
        let mid = 0.5 * (lo + hi);
        if mid == lo || mid == hi {
            break;
        }
        if regularized_beta(a, b, mid) < p {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok(0.5 * (lo + hi))
}

pub(crate) fn t_cdf(df: f64, value: f64) -> DFResult<f64> {
    if !positive(df) {
        return Err(invalid("df must be > 0"));
    }
    if value.is_nan() {
        return Ok(f64::NAN);
    }
    if value == 0.0 {
        return Ok(0.5);
    }
    let v2 = value * value;
    // Use the form that keeps its beta argument away from 1.
    Ok(if v2 < df {
        let half = regularized_beta(0.5, df / 2.0, v2 / (df + v2));
        0.5 + 0.5 * value.signum() * half
    } else {
        let tail = 0.5 * regularized_beta(df / 2.0, 0.5, df / (df + v2));
        if value > 0.0 {
            1.0 - tail
        } else {
            tail
        }
    })
}

pub(crate) fn t_pdf(df: f64, value: f64) -> DFResult<f64> {
    if !positive(df) {
        return Err(invalid("df must be > 0"));
    }
    let ln_density = libm::lgamma((df + 1.0) / 2.0)
        - libm::lgamma(df / 2.0)
        - 0.5 * (df * std::f64::consts::PI).ln()
        - (df + 1.0) / 2.0 * (value * value / df).ln_1p();
    Ok(ln_density.exp())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(actual: f64, expected: f64, tol: f64) {
        assert!(
            (actual - expected).abs() <= tol,
            "{actual} != {expected} (tol {tol})"
        );
    }

    #[test]
    fn normal_cdf_matches_known_values() {
        close(
            normal_cdf(0.0, 1.0, 1.96).unwrap(),
            0.975_002_104_851_779_5,
            1e-14,
        );
        close(normal_cdf(0.0, 1.0, 0.0).unwrap(), 0.5, 1e-16);
        close(normal_cdf(10.0, 2.0, 10.0).unwrap(), 0.5, 1e-16);
        assert_eq!(normal_cdf(0.0, 1.0, 100.0).unwrap(), 1.0);
        assert_eq!(normal_cdf(0.0, 1.0, f64::NEG_INFINITY).unwrap(), 0.0);
        assert!(normal_cdf(0.0, 0.0, 1.0).is_err());
        assert!(normal_cdf(0.0, -1.0, 1.0).is_err());
    }

    #[test]
    fn inverse_normal_cdf_round_trips() {
        close(
            inverse_normal_cdf(0.0, 1.0, 0.975_002_104_851_779_5).unwrap(),
            1.96,
            1e-12,
        );
        assert_eq!(inverse_normal_cdf(0.0, 1.0, 0.5).unwrap(), 0.0);
        close(inverse_normal_cdf(5.0, 2.0, 0.5).unwrap(), 5.0, 1e-15);
        for p in [1e-10, 1e-4, 0.01, 0.1, 0.3, 0.5, 0.77, 0.99, 1.0 - 1e-6] {
            let x = inverse_normal_cdf(0.0, 1.0, p).unwrap();
            close(normal_cdf(0.0, 1.0, x).unwrap(), p, 1e-13 * p.max(1e-3));
        }
        assert_eq!(
            inverse_normal_cdf(0.0, 1.0, 0.0).unwrap(),
            f64::NEG_INFINITY
        );
        assert_eq!(inverse_normal_cdf(0.0, 1.0, 1.0).unwrap(), f64::INFINITY);
        assert!(inverse_normal_cdf(0.0, 1.0, 1.5).is_err());
        assert!(inverse_normal_cdf(0.0, 1.0, -0.1).is_err());
        assert!(inverse_normal_cdf(0.0, 0.0, 0.5).is_err());
    }

    #[test]
    fn beta_cdf_matches_known_values() {
        // I_x(2, 3) = 6x^2 - 8x^3 + 3x^4.
        close(beta_cdf(2.0, 3.0, 0.5).unwrap(), 0.6875, 1e-14);
        close(beta_cdf(1.0, 1.0, 0.3).unwrap(), 0.3, 1e-14);
        close(beta_cdf(0.5, 0.5, 0.5).unwrap(), 0.5, 1e-14);
        assert_eq!(beta_cdf(2.0, 3.0, 0.0).unwrap(), 0.0);
        assert_eq!(beta_cdf(2.0, 3.0, 1.0).unwrap(), 1.0);
        assert!(beta_cdf(0.0, 3.0, 0.5).is_err());
        assert!(beta_cdf(2.0, 3.0, 1.5).is_err());
    }

    #[test]
    fn inverse_beta_cdf_inverts_beta_cdf() {
        close(inverse_beta_cdf(2.0, 3.0, 0.6875).unwrap(), 0.5, 1e-12);
        for (a, b, p) in [(2.0, 3.0, 0.1), (0.5, 0.5, 0.9), (10.0, 4.0, 0.5)] {
            let x = inverse_beta_cdf(a, b, p).unwrap();
            close(beta_cdf(a, b, x).unwrap(), p, 1e-12);
        }
        assert_eq!(inverse_beta_cdf(2.0, 3.0, 0.0).unwrap(), 0.0);
        assert_eq!(inverse_beta_cdf(2.0, 3.0, 1.0).unwrap(), 1.0);
        assert!(inverse_beta_cdf(2.0, 3.0, 2.0).is_err());
        assert!(inverse_beta_cdf(-1.0, 3.0, 0.5).is_err());
    }

    #[test]
    fn t_distribution_matches_known_values() {
        close(t_cdf(10.0, 2.0).unwrap(), 0.963_305_982_6, 1e-9);
        close(t_cdf(10.0, -2.0).unwrap(), 1.0 - 0.963_305_982_6, 1e-9);
        close(t_cdf(5.0, 0.0).unwrap(), 0.5, 1e-16);
        // One degree of freedom is the Cauchy distribution: atan(v)/pi + 1/2.
        close(t_cdf(1.0, 1.0).unwrap(), 0.75, 1e-13);
        close(
            t_cdf(1.0, 3.0).unwrap(),
            3.0_f64.atan() / std::f64::consts::PI + 0.5,
            1e-13,
        );
        close(t_pdf(10.0, 0.0).unwrap(), 0.389_108_383_9, 1e-9);
        close(t_pdf(1.0, 0.0).unwrap(), 1.0 / std::f64::consts::PI, 1e-14);
        assert!(t_cdf(0.0, 1.0).is_err());
        assert!(t_pdf(-1.0, 1.0).is_err());
    }
}
