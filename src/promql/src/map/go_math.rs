// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The Go `math` functions whose results on amd64 differ from Rust's standard library beyond
//! the comparison tolerance (relative 1e-10), or in whether they overflow or keep their sign.
//! Everything else uses the standard library. Inputs where the standard library fails, all in
//! `go_math_cases.txt`:
//!
//! - `exp`, `sinh`, `cosh`: +Inf from `exp(709.44)` on, where libm is finite up to 709.78.
//! - `log`, `log10`: `log_amd64.s` does not normalize subnormals (`log(5e-324)` is -709.09,
//!   not -744.44).
//! - `log2`: `log(frac) / ln 2 + exp` cancels near 1 (`log2(1.0000000000882365)`).
//! - `acos`: `π/2 - asin(x)` cancels near 1 (`acos(0.999999992587986)`).
//! - `sin`, `cos`, `tan`: `trigReduce` loses precision from 2^29 on (`sin(1.0446683604636763e12)`).
//! - `pow`: repeated squaring accumulates error (`pow(1.0000006850274261, 9.80019037e8)`), and
//!   subnormal bases go through `log`.
//! - `atan2`: `atan(y / x)` underflows to +0 and picks the wrong half-turn
//!   (`atan2(-1e-300, -1e300)` is +π).
//!
//! `asin` and `atan` are kept only as dependencies of `acos` and `atan2`.

#![allow(clippy::excessive_precision)]

use std::f64::consts::{FRAC_1_SQRT_2, FRAC_PI_2, FRAC_PI_4, LOG2_E, PI};

#[cfg(test)]
mod tests;

const MASK: u64 = 0x7FF;
const SHIFT: u32 = 64 - 11 - 1;
const BIAS: i64 = 1023;

// Go folds constant expressions exactly before rounding, so derived constants are spelled
// as Go's bits where a Rust expression would round differently.
const FOUR_OVER_PI: f64 = f64::from_bits(0x3ff45f306dc9c883);
const THREE_PI_4: f64 = f64::from_bits(0x4002d97c7f3321d2);
const INV_LN10: f64 = f64::from_bits(0x3fdbcb7b1526e50e);
const INV_LN2: f64 = f64::from_bits(0x3ff71547652b82fe);

/// Go `math.Exp` on amd64 overflows once `x * log2(e)` rounds to 1024, from about 709.44
/// (`exp_amd64.s`); below that libm is within the tolerance.
pub(crate) fn exp(x: f64) -> f64 {
    if x.is_finite() && (LOG2_E * x).round_ties_even() >= 1024.0 {
        f64::INFINITY
    } else {
        x.exp()
    }
}

/// Go `math.Mod`. Both Go's loop and C `fmod` compute the exact remainder with the sign of
/// `x`, and return NaN for a zero divisor, an infinite dividend or NaN input.
pub(crate) fn fmod(x: f64, y: f64) -> f64 {
    x % y
}

/// Go `math.Pow`.
pub(crate) fn pow(x: f64, y: f64) -> f64 {
    if y == 0.0 || x == 1.0 {
        return 1.0;
    }
    if y == 1.0 {
        return x;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 {
        if y < 0.0 {
            return if x.is_sign_negative() && is_odd_int(y) {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };
        }
        return if x.is_sign_negative() && is_odd_int(y) {
            x
        } else {
            0.0
        };
    }
    if y.is_infinite() {
        return if x == -1.0 {
            1.0
        } else if (x.abs() < 1.0) == (y > 0.0) {
            0.0
        } else {
            f64::INFINITY
        };
    }
    if x.is_infinite() {
        if x < 0.0 {
            return pow(1.0 / x, -y);
        }
        return if y < 0.0 { 0.0 } else { f64::INFINITY };
    }
    if y == 0.5 {
        return x.sqrt();
    }
    if y == -0.5 {
        return 1.0 / x.sqrt();
    }

    let (mut yi, mut yf) = (y.abs().trunc(), y.abs().fract());
    if yf != 0.0 && x < 0.0 {
        return f64::NAN;
    }
    if yi >= (1u64 << 63) as f64 {
        return if x == -1.0 {
            1.0
        } else if (x.abs() < 1.0) == (y > 0.0) {
            0.0
        } else {
            f64::INFINITY
        };
    }

    let mut a1 = 1.0;
    let mut ae: i64 = 0;
    if yf != 0.0 {
        if yf > 0.5 {
            yf -= 1.0;
            yi += 1.0;
        }
        a1 = exp(yf * log(x));
    }

    let (mut x1, mut xe) = frexp(x);
    let mut i = yi as i64;
    while i != 0 {
        if !(-(1 << 12)..=(1 << 12)).contains(&xe) {
            ae += xe;
            break;
        }
        if i & 1 == 1 {
            a1 *= x1;
            ae += xe;
        }
        x1 *= x1;
        xe <<= 1;
        if x1 < 0.5 {
            x1 += x1;
            xe -= 1;
        }
        i >>= 1;
    }

    if y < 0.0 {
        a1 = 1.0 / a1;
        ae = -ae;
    }
    ldexp(a1, ae)
}

fn is_odd_int(x: f64) -> bool {
    if x.abs() >= (1u64 << 53) as f64 {
        return false;
    }
    x.fract() == 0.0 && (x.trunc() as i64) & 1 == 1
}

fn normalize(x: f64) -> (f64, i64) {
    if x.abs() < f64::MIN_POSITIVE {
        return (x * (1u64 << 52) as f64, -52);
    }
    (x, 0)
}

/// Go `math.Frexp`.
fn frexp(f: f64) -> (f64, i64) {
    if f == 0.0 || f.is_infinite() || f.is_nan() {
        return (f, 0);
    }
    let (f, mut exp) = normalize(f);
    let mut x = f.to_bits();
    exp += ((x >> SHIFT) & MASK) as i64 - BIAS + 1;
    x &= !(MASK << SHIFT);
    x |= ((-1 + BIAS) as u64) << SHIFT;
    (f64::from_bits(x), exp)
}

/// Go `math.Ldexp`.
fn ldexp(frac: f64, exp: i64) -> f64 {
    if frac == 0.0 || frac.is_infinite() || frac.is_nan() {
        return frac;
    }
    let (frac, e) = normalize(frac);
    let mut exp = exp + e;
    let mut x = frac.to_bits();
    exp += ((x >> SHIFT) & MASK) as i64 - BIAS;
    if exp < -1075 {
        return 0.0_f64.copysign(frac);
    }
    if exp > 1023 {
        return if frac < 0.0 {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    let mut m = 1.0;
    if exp < -1022 {
        exp += 53;
        m = 1.0 / (1u64 << 53) as f64;
    }
    x &= !(MASK << SHIFT);
    x |= ((exp + BIAS) as u64) << SHIFT;
    m * f64::from_bits(x)
}

/// Go `math.Atan2`.
pub(crate) fn atan2(y: f64, x: f64) -> f64 {
    if y.is_nan() || x.is_nan() {
        return f64::NAN;
    }
    if y == 0.0 {
        if x >= 0.0 && !x.is_sign_negative() {
            return 0.0_f64.copysign(y);
        }
        return PI.copysign(y);
    }
    if x == 0.0 {
        return FRAC_PI_2.copysign(y);
    }
    if x.is_infinite() {
        if x > 0.0 {
            return if y.is_infinite() {
                FRAC_PI_4.copysign(y)
            } else {
                0.0_f64.copysign(y)
            };
        }
        return if y.is_infinite() {
            THREE_PI_4.copysign(y)
        } else {
            PI.copysign(y)
        };
    }
    if y.is_infinite() {
        return FRAC_PI_2.copysign(y);
    }
    let q = atan(y / x);
    if x < 0.0 {
        if q <= 0.0 {
            return q + PI;
        }
        return q - PI;
    }
    q
}

/// Go `math.Atan`.
fn atan(x: f64) -> f64 {
    if x == 0.0 {
        return x;
    }
    if x > 0.0 { satan(x) } else { -satan(-x) }
}

fn satan(x: f64) -> f64 {
    const MOREBITS: f64 = 6.123233995736765886130e-17;
    const TAN3PIO8: f64 = 2.41421356237309504880;
    if x <= 0.66 {
        return xatan(x);
    }
    if x > TAN3PIO8 {
        return FRAC_PI_2 - xatan(1.0 / x) + MOREBITS;
    }
    FRAC_PI_4 + xatan((x - 1.0) / (x + 1.0)) + 0.5 * MOREBITS
}

fn xatan(x: f64) -> f64 {
    const P0: f64 = -8.750608600031904122785e-01;
    const P1: f64 = -1.615753718733365076637e+01;
    const P2: f64 = -7.500855792314704667340e+01;
    const P3: f64 = -1.228866684490136173410e+02;
    const P4: f64 = -6.485021904942025371773e+01;
    const Q0: f64 = 2.485846490142306297962e+01;
    const Q1: f64 = 1.650270098316988542046e+02;
    const Q2: f64 = 4.328810604912902668951e+02;
    const Q3: f64 = 4.853903996359136964868e+02;
    const Q4: f64 = 1.945506571482613964425e+02;
    let mut z = x * x;
    z = z * ((((P0 * z + P1) * z + P2) * z + P3) * z + P4)
        / (((((z + Q0) * z + Q1) * z + Q2) * z + Q3) * z + Q4);
    x * z + x
}

/// Go `math.Asin`.
fn asin(x: f64) -> f64 {
    if x == 0.0 {
        return x;
    }
    let (x, sign) = if x < 0.0 { (-x, true) } else { (x, false) };
    if x > 1.0 {
        return f64::NAN;
    }
    let mut temp = (1.0 - x * x).sqrt();
    if x > 0.7 {
        temp = FRAC_PI_2 - satan(temp / x);
    } else {
        temp = satan(x / temp);
    }
    if sign { -temp } else { temp }
}

/// Go `math.Acos`.
pub(crate) fn acos(x: f64) -> f64 {
    FRAC_PI_2 - asin(x)
}

const PI4A: f64 = 7.85398125648498535156e-1;
const PI4B: f64 = 3.77489470793079817668e-8;
const PI4C: f64 = 2.69515142907905952645e-15;
const REDUCE_THRESHOLD: f64 = (1u64 << 29) as f64;

const SIN: [f64; 6] = [
    1.58962301576546568060e-10,
    -2.50507477628578072866e-8,
    2.75573136213857245213e-6,
    -1.98412698295895385996e-4,
    8.33333333332211858878e-3,
    -1.66666666666666307295e-1,
];
const COS: [f64; 6] = [
    -1.13585365213876817300e-11,
    2.08757008419747316778e-9,
    -2.75573141792967388112e-7,
    2.48015872888517045348e-5,
    -1.38888888888730564116e-3,
    4.16666666666665929218e-2,
];

/// The octant and the reduced argument of a non-negative finite `x`.
fn reduce(x: f64) -> (u64, f64) {
    if x >= REDUCE_THRESHOLD {
        return trig_reduce(x);
    }
    let mut j = (x * FOUR_OVER_PI) as u64;
    let mut y = j as f64;
    if j & 1 == 1 {
        j += 1;
        y += 1.0;
    }
    (j, ((x - y * PI4A) - y * PI4B) - y * PI4C)
}

fn sin_poly(z: f64, zz: f64) -> f64 {
    z + z
        * zz
        * ((((((SIN[0] * zz) + SIN[1]) * zz + SIN[2]) * zz + SIN[3]) * zz + SIN[4]) * zz + SIN[5])
}

fn cos_poly(zz: f64) -> f64 {
    1.0 - 0.5 * zz
        + zz * zz
            * ((((((COS[0] * zz) + COS[1]) * zz + COS[2]) * zz + COS[3]) * zz + COS[4]) * zz
                + COS[5])
}

/// Go `math.Cos`.
pub(crate) fn cos(x: f64) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return f64::NAN;
    }
    let (mut j, z) = reduce(x.abs());
    j &= 7;
    let mut sign = false;
    if j > 3 {
        j -= 4;
        sign = !sign;
    }
    if j > 1 {
        sign = !sign;
    }
    let zz = z * z;
    let y = if j == 1 || j == 2 {
        sin_poly(z, zz)
    } else {
        cos_poly(zz)
    };
    if sign { -y } else { y }
}

/// Go `math.Sin`.
pub(crate) fn sin(x: f64) -> f64 {
    if x == 0.0 || x.is_nan() {
        return x;
    }
    if x.is_infinite() {
        return f64::NAN;
    }
    let (x, mut sign) = if x < 0.0 { (-x, true) } else { (x, false) };
    let (mut j, z) = reduce(x);
    j &= 7;
    if j > 3 {
        sign = !sign;
        j -= 4;
    }
    let zz = z * z;
    let y = if j == 1 || j == 2 {
        cos_poly(zz)
    } else {
        sin_poly(z, zz)
    };
    if sign { -y } else { y }
}

/// Go `math.Tan`.
pub(crate) fn tan(x: f64) -> f64 {
    const P: [f64; 3] = [
        -1.30936939181383777646e4,
        1.15351664838587416140e6,
        -1.79565251976484877988e7,
    ];
    const Q: [f64; 5] = [
        1.00000000000000000000e0,
        1.36812963470692954678e4,
        -1.32089234440210967447e6,
        2.50083801823357915839e7,
        -5.38695755929454629881e7,
    ];
    if x == 0.0 || x.is_nan() {
        return x;
    }
    if x.is_infinite() {
        return f64::NAN;
    }
    let (x, sign) = if x < 0.0 { (-x, true) } else { (x, false) };
    let (j, z) = reduce(x);
    let zz = z * z;
    let mut y = if zz > 1e-14 {
        z + z
            * (zz * (((P[0] * zz) + P[1]) * zz + P[2])
                / ((((zz + Q[1]) * zz + Q[2]) * zz + Q[3]) * zz + Q[4]))
    } else {
        z
    };
    if j & 2 == 2 {
        y = -1.0 / y;
    }
    if sign { -y } else { y }
}

const M_PI4: [u64; 20] = [
    0x0000000000000001,
    0x45f306dc9c882a53,
    0xf84eafa3ea69bb81,
    0xb6c52b3278872083,
    0xfca2c757bd778ac3,
    0x6e48dc74849ba5c0,
    0x0c925dd413a32439,
    0xfc3bd63962534e7d,
    0xd1046bea5d768909,
    0xd338e04d68befc82,
    0x7323ac7306a673e9,
    0x3908bf177bf25076,
    0x3ff12fffbc0b301f,
    0xde5e2316b414da3e,
    0xda6cfd9e4f96136e,
    0x9e8c7ecd3cbfd45a,
    0xea4f758fd7cbe2f6,
    0x7a0e73ef14a525d4,
    0xd7f6bf623f1aba10,
    0xac06608df8f6d757,
];

/// Go `trigReduce`: Payne–Hanek reduction for `x >= 2^29`.
fn trig_reduce(x: f64) -> (u64, f64) {
    if x < FRAC_PI_4 {
        return (0, x);
    }
    let mut ix = x.to_bits();
    let exp = ((ix >> SHIFT) & MASK) as i64 - BIAS - SHIFT as i64;
    ix &= !(MASK << SHIFT);
    ix |= 1 << SHIFT;
    let (digit, bitshift) = (((exp + 61) / 64) as usize, ((exp + 61) % 64) as u32);
    let join = |hi: u64, lo: u64| shl(hi, bitshift) | shr(lo, 64 - bitshift);
    let z0 = join(M_PI4[digit], M_PI4[digit + 1]);
    let z1 = join(M_PI4[digit + 1], M_PI4[digit + 2]);
    let z2 = join(M_PI4[digit + 2], M_PI4[digit + 3]);
    let z2hi = ((z2 as u128 * ix as u128) >> 64) as u64;
    let z1prod = z1 as u128 * ix as u128;
    let (z1hi, z1lo) = ((z1prod >> 64) as u64, z1prod as u64);
    let z0lo = z0.wrapping_mul(ix);
    let (lo, carry) = z1lo.overflowing_add(z2hi);
    let mut hi = z0lo.wrapping_add(z1hi).wrapping_add(carry as u64);
    let mut j = hi >> 61;
    hi = (hi << 3) | (lo >> 61);
    let lz = hi.leading_zeros();
    let e = (BIAS as u64).wrapping_sub(lz as u64 + 1);
    hi = shl(hi, lz + 1) | shr(lo, 64u32.wrapping_sub(lz + 1));
    hi >>= 64 - SHIFT;
    hi |= e << SHIFT;
    let mut z = f64::from_bits(hi);
    if j & 1 == 1 {
        j += 1;
        j &= 7;
        z -= 1.0;
    }
    (j, z * FRAC_PI_4)
}

// Go shifts by 64 or more produce 0 instead of panicking.
fn shl(x: u64, n: u32) -> u64 {
    x.checked_shl(n).unwrap_or(0)
}

fn shr(x: u64, n: u32) -> u64 {
    x.checked_shr(n).unwrap_or(0)
}

/// Go `math.Sinh`, which is `exp(|x|) / 2` above 21 and so overflows with [`exp`].
pub(crate) fn sinh(x: f64) -> f64 {
    if x.abs() > 21.0 {
        (exp(x.abs()) * 0.5).copysign(x)
    } else {
        x.sinh()
    }
}

/// Go `math.Cosh`, which is `exp(|x|) / 2` above 21 and so overflows with [`exp`].
pub(crate) fn cosh(x: f64) -> f64 {
    if x.abs() > 21.0 {
        exp(x.abs()) * 0.5
    } else {
        x.cosh()
    }
}

const LN2_HI: f64 = 6.93147180369123816490e-01;
const LN2_LO: f64 = 1.90821492927058770002e-10;
const L1: f64 = 6.666666666666735130e-01;
const L2: f64 = 3.999999999940941908e-01;
const L3: f64 = 2.857142874366239149e-01;
const L4: f64 = 2.222219843214978396e-01;
const L5: f64 = 1.818357216161805012e-01;
const L6: f64 = 1.531383769920937332e-01;
const L7: f64 = 1.479819860511658591e-01;

/// Go `math.Log`, following `log_amd64.s` operation by operation. Its inline frexp does not
/// normalize subnormal input, which changes the result there.
pub(crate) fn log(x: f64) -> f64 {
    let bits = x.to_bits();
    if bits & !(1 << 63) == 0 {
        return f64::NEG_INFINITY;
    }
    if (bits as i64) < 0 {
        return f64::NAN;
    }
    if bits >= f64::INFINITY.to_bits() {
        return x;
    }
    let mut f1 = f64::from_bits((bits & 0x000FFFFFFFFFFFFF) | 0.5f64.to_bits());
    let mut k = (((bits >> 52) & 0x7FF) as i32 - 0x3FE) as f64;
    // CMPSD predicate 5 is "not less than": Sqrt2/2 >= f1.
    if FRAC_1_SQRT_2 >= f1 {
        k -= 1.0;
        f1 *= 2.0;
    }
    let f = f1 - 1.0;
    let s = f / (2.0 + f);
    let s2 = s * s;
    let s4 = s2 * s2;
    let t1 = s2 * (((L7 * s4 + L5) * s4 + L3) * s4 + L1);
    let t2 = s4 * ((L6 * s4 + L4) * s4 + L2);
    let r = t1 + t2;
    let hfsq = 0.5 * f * f;
    k * LN2_HI - ((hfsq - (s * (hfsq + r) + k * LN2_LO)) - f)
}

/// Go `math.Log2`.
pub(crate) fn log2(x: f64) -> f64 {
    // Go returns `exp - 1` for `frac == 0.5`, which is also what the general formula gives.
    let (frac, exp) = frexp(x);
    log(frac) * INV_LN2 + exp as f64
}

/// Go `math.Log10`.
pub(crate) fn log10(x: f64) -> f64 {
    log(x) * INV_LN10
}
