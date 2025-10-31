import { i as M, a as _, b as D, c as X, d as Z, e as Un, f as Fr, g as He, o as Dr, h as V, n as cn, j as Yn, k as Gr, S as Ln, l as se } from "./mermaidAPI-67f627de.js";
import { j as ke, S as C, C as L, f as Br, D as nn, E as Ur, L as Hn, M as Yr, F as qe, h as kn, k as Mn } from "./utils-8ea37061.js";
var Hr = "[object Symbol]";
function R(n) {
  return typeof n == "symbol" || M(n) && ke(n) == Hr;
}
function H(n, e) {
  for (var r = -1, t = n == null ? 0 : n.length, i = Array(t); ++r < t; )
    i[r] = e(n[r], r, n);
  return i;
}
var kr = 1 / 0, fe = C ? C.prototype : void 0, de = fe ? fe.toString : void 0;
function Ke(n) {
  if (typeof n == "string")
    return n;
  if (_(n))
    return H(n, Ke) + "";
  if (R(n))
    return de ? de.call(n) : "";
  var e = n + "";
  return e == "0" && 1 / n == -kr ? "-0" : e;
}
var qr = /\s/;
function Kr(n) {
  for (var e = n.length; e-- && qr.test(n.charAt(e)); )
    ;
  return e;
}
var Wr = /^\s+/;
function Xr(n) {
  return n && n.slice(0, Kr(n) + 1).replace(Wr, "");
}
var ce = 0 / 0, Zr = /^[-+]0x[0-9a-f]+$/i, Vr = /^0b[01]+$/i, jr = /^0o[0-7]+$/i, zr = parseInt;
function Jr(n) {
  if (typeof n == "number")
    return n;
  if (R(n))
    return ce;
  if (L(n)) {
    var e = typeof n.valueOf == "function" ? n.valueOf() : n;
    n = L(e) ? e + "" : e;
  }
  if (typeof n != "string")
    return n === 0 ? n : +n;
  n = Xr(n);
  var r = Vr.test(n);
  return r || jr.test(n) ? zr(n.slice(2), r ? 2 : 8) : Zr.test(n) ? ce : +n;
}
var le = 1 / 0, Qr = 17976931348623157e292;
function fn(n) {
  if (!n)
    return n === 0 ? n : 0;
  if (n = Jr(n), n === le || n === -le) {
    var e = n < 0 ? -1 : 1;
    return e * Qr;
  }
  return n === n ? n : 0;
}
function nt(n) {
  var e = fn(n), r = e % 1;
  return e === e ? r ? e - r : e : 0;
}
function G(n) {
  return n;
}
var he = Object.create, et = function() {
  function n() {
  }
  return function(e) {
    if (!L(e))
      return {};
    if (he)
      return he(e);
    n.prototype = e;
    var r = new n();
    return n.prototype = void 0, r;
  };
}();
const rt = et;
function tt(n, e, r) {
  switch (r.length) {
    case 0:
      return n.call(e);
    case 1:
      return n.call(e, r[0]);
    case 2:
      return n.call(e, r[0], r[1]);
    case 3:
      return n.call(e, r[0], r[1], r[2]);
  }
  return n.apply(e, r);
}
function it() {
}
function We(n, e) {
  var r = -1, t = n.length;
  for (e || (e = Array(t)); ++r < t; )
    e[r] = n[r];
  return e;
}
var at = 800, ot = 16, ut = Date.now;
function st(n) {
  var e = 0, r = 0;
  return function() {
    var t = ut(), i = ot - (t - r);
    if (r = t, i > 0) {
      if (++e >= at)
        return arguments[0];
    } else
      e = 0;
    return n.apply(void 0, arguments);
  };
}
function Y(n) {
  return function() {
    return n;
  };
}
var ft = function() {
  try {
    var n = Br(Object, "defineProperty");
    return n({}, "", {}), n;
  } catch {
  }
}();
const ln = ft;
var dt = ln ? function(n, e) {
  return ln(n, "toString", {
    configurable: !0,
    enumerable: !1,
    value: Y(e),
    writable: !0
  });
} : G;
const ct = dt;
var lt = st(ct);
const Xe = lt;
function Ze(n, e) {
  for (var r = -1, t = n == null ? 0 : n.length; ++r < t && e(n[r], r, n) !== !1; )
    ;
  return n;
}
function Ve(n, e, r, t) {
  for (var i = n.length, a = r + (t ? 1 : -1); t ? a-- : ++a < i; )
    if (e(n[a], a, n))
      return a;
  return -1;
}
function ht(n) {
  return n !== n;
}
function vt(n, e, r) {
  for (var t = r - 1, i = n.length; ++t < i; )
    if (n[t] === e)
      return t;
  return -1;
}
function gt(n, e, r) {
  return e === e ? vt(n, e, r) : Ve(n, ht, r);
}
function pt(n, e) {
  var r = n == null ? 0 : n.length;
  return !!r && gt(n, e, 0) > -1;
}
var bt = 9007199254740991, wt = /^(?:0|[1-9]\d*)$/;
function pn(n, e) {
  var r = typeof n;
  return e = e ?? bt, !!e && (r == "number" || r != "symbol" && wt.test(n)) && n > -1 && n % 1 == 0 && n < e;
}
function bn(n, e, r) {
  e == "__proto__" && ln ? ln(n, e, {
    configurable: !0,
    enumerable: !0,
    value: r,
    writable: !0
  }) : n[e] = r;
}
var mt = Object.prototype, _t = mt.hasOwnProperty;
function wn(n, e, r) {
  var t = n[e];
  (!(_t.call(n, e) && nn(t, r)) || r === void 0 && !(e in n)) && bn(n, e, r);
}
function en(n, e, r, t) {
  var i = !r;
  r || (r = {});
  for (var a = -1, o = e.length; ++a < o; ) {
    var u = e[a], s = t ? t(r[u], n[u], u, r, n) : void 0;
    s === void 0 && (s = n[u]), i ? bn(r, u, s) : wn(r, u, s);
  }
  return r;
}
var ve = Math.max;
function je(n, e, r) {
  return e = ve(e === void 0 ? n.length - 1 : e, 0), function() {
    for (var t = arguments, i = -1, a = ve(t.length - e, 0), o = Array(a); ++i < a; )
      o[i] = t[e + i];
    i = -1;
    for (var u = Array(e + 1); ++i < e; )
      u[i] = t[i];
    return u[e] = r(o), tt(n, this, u);
  };
}
function mn(n, e) {
  return Xe(je(n, e, G), n + "");
}
function j(n, e, r) {
  if (!L(r))
    return !1;
  var t = typeof e;
  return (t == "number" ? D(r) && pn(e, r.length) : t == "string" && e in r) ? nn(r[e], n) : !1;
}
function Et(n) {
  return mn(function(e, r) {
    var t = -1, i = r.length, a = i > 1 ? r[i - 1] : void 0, o = i > 2 ? r[2] : void 0;
    for (a = n.length > 3 && typeof a == "function" ? (i--, a) : void 0, o && j(r[0], r[1], o) && (a = i < 3 ? void 0 : a, i = 1), e = Object(e); ++t < i; ) {
      var u = r[t];
      u && n(e, u, t, a);
    }
    return e;
  });
}
function yt(n, e) {
  for (var r = -1, t = Array(n); ++r < n; )
    t[r] = e(r);
  return t;
}
var xt = Object.prototype, Tt = xt.hasOwnProperty;
function ze(n, e) {
  var r = _(n), t = !r && X(n), i = !r && !t && Z(n), a = !r && !t && !i && Un(n), o = r || t || i || a, u = o ? yt(n.length, String) : [], s = u.length;
  for (var f in n)
    (e || Tt.call(n, f)) && !(o && // Safari 9 has enumerable `arguments.length` in strict mode.
    (f == "length" || // Node.js 0.10 has enumerable non-index properties on buffers.
    i && (f == "offset" || f == "parent") || // PhantomJS 2 has enumerable non-index properties on typed arrays.
    a && (f == "buffer" || f == "byteLength" || f == "byteOffset") || // Skip index properties.
    pn(f, s))) && u.push(f);
  return u;
}
function T(n) {
  return D(n) ? ze(n) : Fr(n);
}
function Ot(n) {
  var e = [];
  if (n != null)
    for (var r in Object(n))
      e.push(r);
  return e;
}
var Lt = Object.prototype, At = Lt.hasOwnProperty;
function Pt(n) {
  if (!L(n))
    return Ot(n);
  var e = He(n), r = [];
  for (var t in n)
    t == "constructor" && (e || !At.call(n, t)) || r.push(t);
  return r;
}
function B(n) {
  return D(n) ? ze(n, !0) : Pt(n);
}
var Nt = /\.|\[(?:[^[\]]*|(["'])(?:(?!\1)[^\\]|\\.)*?\1)\]/, Ct = /^\w*$/;
function qn(n, e) {
  if (_(n))
    return !1;
  var r = typeof n;
  return r == "number" || r == "symbol" || r == "boolean" || n == null || R(n) ? !0 : Ct.test(n) || !Nt.test(n) || e != null && n in Object(e);
}
var $t = 500;
function It(n) {
  var e = Ur(n, function(t) {
    return r.size === $t && r.clear(), t;
  }), r = e.cache;
  return e;
}
var St = /[^.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|(["'])((?:(?!\2)[^\\]|\\.)*?)\2)\]|(?=(?:\.|\[\])(?:\.|\[\]|$))/g, Mt = /\\(\\)?/g, Rt = It(function(n) {
  var e = [];
  return n.charCodeAt(0) === 46 && e.push(""), n.replace(St, function(r, t, i, a) {
    e.push(i ? a.replace(Mt, "$1") : t || r);
  }), e;
});
const Ft = Rt;
function Je(n) {
  return n == null ? "" : Ke(n);
}
function _n(n, e) {
  return _(n) ? n : qn(n, e) ? [n] : Ft(Je(n));
}
var Dt = 1 / 0;
function rn(n) {
  if (typeof n == "string" || R(n))
    return n;
  var e = n + "";
  return e == "0" && 1 / n == -Dt ? "-0" : e;
}
function En(n, e) {
  e = _n(e, n);
  for (var r = 0, t = e.length; n != null && r < t; )
    n = n[rn(e[r++])];
  return r && r == t ? n : void 0;
}
function Gt(n, e, r) {
  var t = n == null ? void 0 : En(n, e);
  return t === void 0 ? r : t;
}
function Kn(n, e) {
  for (var r = -1, t = e.length, i = n.length; ++r < t; )
    n[i + r] = e[r];
  return n;
}
var ge = C ? C.isConcatSpreadable : void 0;
function Bt(n) {
  return _(n) || X(n) || !!(ge && n && n[ge]);
}
function yn(n, e, r, t, i) {
  var a = -1, o = n.length;
  for (r || (r = Bt), i || (i = []); ++a < o; ) {
    var u = n[a];
    e > 0 && r(u) ? e > 1 ? yn(u, e - 1, r, t, i) : Kn(i, u) : t || (i[i.length] = u);
  }
  return i;
}
function q(n) {
  var e = n == null ? 0 : n.length;
  return e ? yn(n, 1) : [];
}
function Ut(n) {
  return Xe(je(n, void 0, q), n + "");
}
var Yt = Dr(Object.getPrototypeOf, Object);
const Wn = Yt;
var Ht = "[object Object]", kt = Function.prototype, qt = Object.prototype, Qe = kt.toString, Kt = qt.hasOwnProperty, Wt = Qe.call(Object);
function Xt(n) {
  if (!M(n) || ke(n) != Ht)
    return !1;
  var e = Wn(n);
  if (e === null)
    return !0;
  var r = Kt.call(e, "constructor") && e.constructor;
  return typeof r == "function" && r instanceof r && Qe.call(r) == Wt;
}
function Zt(n, e, r, t) {
  var i = -1, a = n == null ? 0 : n.length;
  for (t && a && (r = n[++i]); ++i < a; )
    r = e(r, n[i], i, n);
  return r;
}
function Vt() {
  this.__data__ = new Hn(), this.size = 0;
}
function jt(n) {
  var e = this.__data__, r = e.delete(n);
  return this.size = e.size, r;
}
function zt(n) {
  return this.__data__.get(n);
}
function Jt(n) {
  return this.__data__.has(n);
}
var Qt = 200;
function ni(n, e) {
  var r = this.__data__;
  if (r instanceof Hn) {
    var t = r.__data__;
    if (!Yr || t.length < Qt - 1)
      return t.push([n, e]), this.size = ++r.size, this;
    r = this.__data__ = new qe(t);
  }
  return r.set(n, e), this.size = r.size, this;
}
function O(n) {
  var e = this.__data__ = new Hn(n);
  this.size = e.size;
}
O.prototype.clear = Vt;
O.prototype.delete = jt;
O.prototype.get = zt;
O.prototype.has = Jt;
O.prototype.set = ni;
function ei(n, e) {
  return n && en(e, T(e), n);
}
function ri(n, e) {
  return n && en(e, B(e), n);
}
var nr = typeof exports == "object" && exports && !exports.nodeType && exports, pe = nr && typeof module == "object" && module && !module.nodeType && module, ti = pe && pe.exports === nr, be = ti ? kn.Buffer : void 0, we = be ? be.allocUnsafe : void 0;
function er(n, e) {
  if (e)
    return n.slice();
  var r = n.length, t = we ? we(r) : new n.constructor(r);
  return n.copy(t), t;
}
function rr(n, e) {
  for (var r = -1, t = n == null ? 0 : n.length, i = 0, a = []; ++r < t; ) {
    var o = n[r];
    e(o, r, n) && (a[i++] = o);
  }
  return a;
}
function tr() {
  return [];
}
var ii = Object.prototype, ai = ii.propertyIsEnumerable, me = Object.getOwnPropertySymbols, oi = me ? function(n) {
  return n == null ? [] : (n = Object(n), rr(me(n), function(e) {
    return ai.call(n, e);
  }));
} : tr;
const Xn = oi;
function ui(n, e) {
  return en(n, Xn(n), e);
}
var si = Object.getOwnPropertySymbols, fi = si ? function(n) {
  for (var e = []; n; )
    Kn(e, Xn(n)), n = Wn(n);
  return e;
} : tr;
const ir = fi;
function di(n, e) {
  return en(n, ir(n), e);
}
function ar(n, e, r) {
  var t = e(n);
  return _(n) ? t : Kn(t, r(n));
}
function Rn(n) {
  return ar(n, T, Xn);
}
function ci(n) {
  return ar(n, B, ir);
}
var li = Object.prototype, hi = li.hasOwnProperty;
function vi(n) {
  var e = n.length, r = new n.constructor(e);
  return e && typeof n[0] == "string" && hi.call(n, "index") && (r.index = n.index, r.input = n.input), r;
}
var gi = kn.Uint8Array;
const hn = gi;
function Zn(n) {
  var e = new n.constructor(n.byteLength);
  return new hn(e).set(new hn(n)), e;
}
function pi(n, e) {
  var r = e ? Zn(n.buffer) : n.buffer;
  return new n.constructor(r, n.byteOffset, n.byteLength);
}
var bi = /\w*$/;
function wi(n) {
  var e = new n.constructor(n.source, bi.exec(n));
  return e.lastIndex = n.lastIndex, e;
}
var _e = C ? C.prototype : void 0, Ee = _e ? _e.valueOf : void 0;
function mi(n) {
  return Ee ? Object(Ee.call(n)) : {};
}
function or(n, e) {
  var r = e ? Zn(n.buffer) : n.buffer;
  return new n.constructor(r, n.byteOffset, n.length);
}
var _i = "[object Boolean]", Ei = "[object Date]", yi = "[object Map]", xi = "[object Number]", Ti = "[object RegExp]", Oi = "[object Set]", Li = "[object String]", Ai = "[object Symbol]", Pi = "[object ArrayBuffer]", Ni = "[object DataView]", Ci = "[object Float32Array]", $i = "[object Float64Array]", Ii = "[object Int8Array]", Si = "[object Int16Array]", Mi = "[object Int32Array]", Ri = "[object Uint8Array]", Fi = "[object Uint8ClampedArray]", Di = "[object Uint16Array]", Gi = "[object Uint32Array]";
function Bi(n, e, r) {
  var t = n.constructor;
  switch (e) {
    case Pi:
      return Zn(n);
    case _i:
    case Ei:
      return new t(+n);
    case Ni:
      return pi(n, r);
    case Ci:
    case $i:
    case Ii:
    case Si:
    case Mi:
    case Ri:
    case Fi:
    case Di:
    case Gi:
      return or(n, r);
    case yi:
      return new t();
    case xi:
    case Li:
      return new t(n);
    case Ti:
      return wi(n);
    case Oi:
      return new t();
    case Ai:
      return mi(n);
  }
}
function ur(n) {
  return typeof n.constructor == "function" && !He(n) ? rt(Wn(n)) : {};
}
var Ui = "[object Map]";
function Yi(n) {
  return M(n) && V(n) == Ui;
}
var ye = cn && cn.isMap, Hi = ye ? Yn(ye) : Yi;
const ki = Hi;
var qi = "[object Set]";
function Ki(n) {
  return M(n) && V(n) == qi;
}
var xe = cn && cn.isSet, Wi = xe ? Yn(xe) : Ki;
const Xi = Wi;
var Zi = 1, Vi = 2, ji = 4, sr = "[object Arguments]", zi = "[object Array]", Ji = "[object Boolean]", Qi = "[object Date]", na = "[object Error]", fr = "[object Function]", ea = "[object GeneratorFunction]", ra = "[object Map]", ta = "[object Number]", dr = "[object Object]", ia = "[object RegExp]", aa = "[object Set]", oa = "[object String]", ua = "[object Symbol]", sa = "[object WeakMap]", fa = "[object ArrayBuffer]", da = "[object DataView]", ca = "[object Float32Array]", la = "[object Float64Array]", ha = "[object Int8Array]", va = "[object Int16Array]", ga = "[object Int32Array]", pa = "[object Uint8Array]", ba = "[object Uint8ClampedArray]", wa = "[object Uint16Array]", ma = "[object Uint32Array]", w = {};
w[sr] = w[zi] = w[fa] = w[da] = w[Ji] = w[Qi] = w[ca] = w[la] = w[ha] = w[va] = w[ga] = w[ra] = w[ta] = w[dr] = w[ia] = w[aa] = w[oa] = w[ua] = w[pa] = w[ba] = w[wa] = w[ma] = !0;
w[na] = w[fr] = w[sa] = !1;
function dn(n, e, r, t, i, a) {
  var o, u = e & Zi, s = e & Vi, f = e & ji;
  if (r && (o = i ? r(n, t, i, a) : r(n)), o !== void 0)
    return o;
  if (!L(n))
    return n;
  var d = _(n);
  if (d) {
    if (o = vi(n), !u)
      return We(n, o);
  } else {
    var l = V(n), h = l == fr || l == ea;
    if (Z(n))
      return er(n, u);
    if (l == dr || l == sr || h && !i) {
      if (o = s || h ? {} : ur(n), !u)
        return s ? di(n, ri(o, n)) : ui(n, ei(o, n));
    } else {
      if (!w[l])
        return i ? n : {};
      o = Bi(n, l, u);
    }
  }
  a || (a = new O());
  var g = a.get(n);
  if (g)
    return g;
  a.set(n, o), Xi(n) ? n.forEach(function(m) {
    o.add(dn(m, e, r, m, n, a));
  }) : ki(n) && n.forEach(function(m, E) {
    o.set(E, dn(m, e, r, E, n, a));
  });
  var v = f ? s ? ci : Rn : s ? B : T, p = d ? void 0 : v(n);
  return Ze(p || n, function(m, E) {
    p && (E = m, m = n[E]), wn(o, E, dn(m, e, r, E, n, a));
  }), o;
}
var _a = 1, Ea = 4;
function ya(n) {
  return dn(n, _a | Ea);
}
var xa = "__lodash_hash_undefined__";
function Ta(n) {
  return this.__data__.set(n, xa), this;
}
function Oa(n) {
  return this.__data__.has(n);
}
function z(n) {
  var e = -1, r = n == null ? 0 : n.length;
  for (this.__data__ = new qe(); ++e < r; )
    this.add(n[e]);
}
z.prototype.add = z.prototype.push = Ta;
z.prototype.has = Oa;
function La(n, e) {
  for (var r = -1, t = n == null ? 0 : n.length; ++r < t; )
    if (e(n[r], r, n))
      return !0;
  return !1;
}
function cr(n, e) {
  return n.has(e);
}
var Aa = 1, Pa = 2;
function lr(n, e, r, t, i, a) {
  var o = r & Aa, u = n.length, s = e.length;
  if (u != s && !(o && s > u))
    return !1;
  var f = a.get(n), d = a.get(e);
  if (f && d)
    return f == e && d == n;
  var l = -1, h = !0, g = r & Pa ? new z() : void 0;
  for (a.set(n, e), a.set(e, n); ++l < u; ) {
    var v = n[l], p = e[l];
    if (t)
      var m = o ? t(p, v, l, e, n, a) : t(v, p, l, n, e, a);
    if (m !== void 0) {
      if (m)
        continue;
      h = !1;
      break;
    }
    if (g) {
      if (!La(e, function(E, I) {
        if (!cr(g, I) && (v === E || i(v, E, r, t, a)))
          return g.push(I);
      })) {
        h = !1;
        break;
      }
    } else if (!(v === p || i(v, p, r, t, a))) {
      h = !1;
      break;
    }
  }
  return a.delete(n), a.delete(e), h;
}
function Na(n) {
  var e = -1, r = Array(n.size);
  return n.forEach(function(t, i) {
    r[++e] = [i, t];
  }), r;
}
function Vn(n) {
  var e = -1, r = Array(n.size);
  return n.forEach(function(t) {
    r[++e] = t;
  }), r;
}
var Ca = 1, $a = 2, Ia = "[object Boolean]", Sa = "[object Date]", Ma = "[object Error]", Ra = "[object Map]", Fa = "[object Number]", Da = "[object RegExp]", Ga = "[object Set]", Ba = "[object String]", Ua = "[object Symbol]", Ya = "[object ArrayBuffer]", Ha = "[object DataView]", Te = C ? C.prototype : void 0, An = Te ? Te.valueOf : void 0;
function ka(n, e, r, t, i, a, o) {
  switch (r) {
    case Ha:
      if (n.byteLength != e.byteLength || n.byteOffset != e.byteOffset)
        return !1;
      n = n.buffer, e = e.buffer;
    case Ya:
      return !(n.byteLength != e.byteLength || !a(new hn(n), new hn(e)));
    case Ia:
    case Sa:
    case Fa:
      return nn(+n, +e);
    case Ma:
      return n.name == e.name && n.message == e.message;
    case Da:
    case Ba:
      return n == e + "";
    case Ra:
      var u = Na;
    case Ga:
      var s = t & Ca;
      if (u || (u = Vn), n.size != e.size && !s)
        return !1;
      var f = o.get(n);
      if (f)
        return f == e;
      t |= $a, o.set(n, e);
      var d = lr(u(n), u(e), t, i, a, o);
      return o.delete(n), d;
    case Ua:
      if (An)
        return An.call(n) == An.call(e);
  }
  return !1;
}
var qa = 1, Ka = Object.prototype, Wa = Ka.hasOwnProperty;
function Xa(n, e, r, t, i, a) {
  var o = r & qa, u = Rn(n), s = u.length, f = Rn(e), d = f.length;
  if (s != d && !o)
    return !1;
  for (var l = s; l--; ) {
    var h = u[l];
    if (!(o ? h in e : Wa.call(e, h)))
      return !1;
  }
  var g = a.get(n), v = a.get(e);
  if (g && v)
    return g == e && v == n;
  var p = !0;
  a.set(n, e), a.set(e, n);
  for (var m = o; ++l < s; ) {
    h = u[l];
    var E = n[h], I = e[h];
    if (t)
      var ue = o ? t(I, E, h, e, n, a) : t(E, I, h, n, e, a);
    if (!(ue === void 0 ? E === I || i(E, I, r, t, a) : ue)) {
      p = !1;
      break;
    }
    m || (m = h == "constructor");
  }
  if (p && !m) {
    var on = n.constructor, un = e.constructor;
    on != un && "constructor" in n && "constructor" in e && !(typeof on == "function" && on instanceof on && typeof un == "function" && un instanceof un) && (p = !1);
  }
  return a.delete(n), a.delete(e), p;
}
var Za = 1, Oe = "[object Arguments]", Le = "[object Array]", sn = "[object Object]", Va = Object.prototype, Ae = Va.hasOwnProperty;
function ja(n, e, r, t, i, a) {
  var o = _(n), u = _(e), s = o ? Le : V(n), f = u ? Le : V(e);
  s = s == Oe ? sn : s, f = f == Oe ? sn : f;
  var d = s == sn, l = f == sn, h = s == f;
  if (h && Z(n)) {
    if (!Z(e))
      return !1;
    o = !0, d = !1;
  }
  if (h && !d)
    return a || (a = new O()), o || Un(n) ? lr(n, e, r, t, i, a) : ka(n, e, s, r, t, i, a);
  if (!(r & Za)) {
    var g = d && Ae.call(n, "__wrapped__"), v = l && Ae.call(e, "__wrapped__");
    if (g || v) {
      var p = g ? n.value() : n, m = v ? e.value() : e;
      return a || (a = new O()), i(p, m, r, t, a);
    }
  }
  return h ? (a || (a = new O()), Xa(n, e, r, t, i, a)) : !1;
}
function jn(n, e, r, t, i) {
  return n === e ? !0 : n == null || e == null || !M(n) && !M(e) ? n !== n && e !== e : ja(n, e, r, t, jn, i);
}
var za = 1, Ja = 2;
function Qa(n, e, r, t) {
  var i = r.length, a = i, o = !t;
  if (n == null)
    return !a;
  for (n = Object(n); i--; ) {
    var u = r[i];
    if (o && u[2] ? u[1] !== n[u[0]] : !(u[0] in n))
      return !1;
  }
  for (; ++i < a; ) {
    u = r[i];
    var s = u[0], f = n[s], d = u[1];
    if (o && u[2]) {
      if (f === void 0 && !(s in n))
        return !1;
    } else {
      var l = new O();
      if (t)
        var h = t(f, d, s, n, e, l);
      if (!(h === void 0 ? jn(d, f, za | Ja, t, l) : h))
        return !1;
    }
  }
  return !0;
}
function hr(n) {
  return n === n && !L(n);
}
function no(n) {
  for (var e = T(n), r = e.length; r--; ) {
    var t = e[r], i = n[t];
    e[r] = [t, i, hr(i)];
  }
  return e;
}
function vr(n, e) {
  return function(r) {
    return r == null ? !1 : r[n] === e && (e !== void 0 || n in Object(r));
  };
}
function eo(n) {
  var e = no(n);
  return e.length == 1 && e[0][2] ? vr(e[0][0], e[0][1]) : function(r) {
    return r === n || Qa(r, n, e);
  };
}
function ro(n, e) {
  return n != null && e in Object(n);
}
function gr(n, e, r) {
  e = _n(e, n);
  for (var t = -1, i = e.length, a = !1; ++t < i; ) {
    var o = rn(e[t]);
    if (!(a = n != null && r(n, o)))
      break;
    n = n[o];
  }
  return a || ++t != i ? a : (i = n == null ? 0 : n.length, !!i && Gr(i) && pn(o, i) && (_(n) || X(n)));
}
function pr(n, e) {
  return n != null && gr(n, e, ro);
}
var to = 1, io = 2;
function ao(n, e) {
  return qn(n) && hr(e) ? vr(rn(n), e) : function(r) {
    var t = Gt(r, n);
    return t === void 0 && t === e ? pr(r, n) : jn(e, t, to | io);
  };
}
function oo(n) {
  return function(e) {
    return e == null ? void 0 : e[n];
  };
}
function uo(n) {
  return function(e) {
    return En(e, n);
  };
}
function so(n) {
  return qn(n) ? oo(rn(n)) : uo(n);
}
function $(n) {
  return typeof n == "function" ? n : n == null ? G : typeof n == "object" ? _(n) ? ao(n[0], n[1]) : eo(n) : so(n);
}
function fo(n) {
  return function(e, r, t) {
    for (var i = -1, a = Object(e), o = t(e), u = o.length; u--; ) {
      var s = o[n ? u : ++i];
      if (r(a[s], s, a) === !1)
        break;
    }
    return e;
  };
}
var co = fo();
const zn = co;
function Jn(n, e) {
  return n && zn(n, e, T);
}
function lo(n, e) {
  return function(r, t) {
    if (r == null)
      return r;
    if (!D(r))
      return n(r, t);
    for (var i = r.length, a = e ? i : -1, o = Object(r); (e ? a-- : ++a < i) && t(o[a], a, o) !== !1; )
      ;
    return r;
  };
}
var ho = lo(Jn);
const xn = ho;
var vo = function() {
  return kn.Date.now();
};
const Pe = vo;
var br = Object.prototype, go = br.hasOwnProperty, po = mn(function(n, e) {
  n = Object(n);
  var r = -1, t = e.length, i = t > 2 ? e[2] : void 0;
  for (i && j(e[0], e[1], i) && (t = 1); ++r < t; )
    for (var a = e[r], o = B(a), u = -1, s = o.length; ++u < s; ) {
      var f = o[u], d = n[f];
      (d === void 0 || nn(d, br[f]) && !go.call(n, f)) && (n[f] = a[f]);
    }
  return n;
});
const bo = po;
function Fn(n, e, r) {
  (r !== void 0 && !nn(n[e], r) || r === void 0 && !(e in n)) && bn(n, e, r);
}
function wr(n) {
  return M(n) && D(n);
}
function Dn(n, e) {
  if (!(e === "constructor" && typeof n[e] == "function") && e != "__proto__")
    return n[e];
}
function wo(n) {
  return en(n, B(n));
}
function mo(n, e, r, t, i, a, o) {
  var u = Dn(n, r), s = Dn(e, r), f = o.get(s);
  if (f) {
    Fn(n, r, f);
    return;
  }
  var d = a ? a(u, s, r + "", n, e, o) : void 0, l = d === void 0;
  if (l) {
    var h = _(s), g = !h && Z(s), v = !h && !g && Un(s);
    d = s, h || g || v ? _(u) ? d = u : wr(u) ? d = We(u) : g ? (l = !1, d = er(s, !0)) : v ? (l = !1, d = or(s, !0)) : d = [] : Xt(s) || X(s) ? (d = u, X(u) ? d = wo(u) : (!L(u) || Mn(u)) && (d = ur(s))) : l = !1;
  }
  l && (o.set(s, d), i(d, s, t, a, o), o.delete(s)), Fn(n, r, d);
}
function mr(n, e, r, t, i) {
  n !== e && zn(e, function(a, o) {
    if (i || (i = new O()), L(a))
      mo(n, e, o, r, mr, t, i);
    else {
      var u = t ? t(Dn(n, o), a, o + "", n, e, i) : void 0;
      u === void 0 && (u = a), Fn(n, o, u);
    }
  }, B);
}
function _o(n, e, r) {
  for (var t = -1, i = n == null ? 0 : n.length; ++t < i; )
    if (r(e, n[t]))
      return !0;
  return !1;
}
function vn(n) {
  var e = n == null ? 0 : n.length;
  return e ? n[e - 1] : void 0;
}
function Qn(n) {
  return typeof n == "function" ? n : G;
}
function c(n, e) {
  var r = _(n) ? Ze : xn;
  return r(n, Qn(e));
}
function Eo(n, e) {
  var r = [];
  return xn(n, function(t, i, a) {
    e(t, i, a) && r.push(t);
  }), r;
}
function P(n, e) {
  var r = _(n) ? rr : Eo;
  return r(n, $(e));
}
function yo(n) {
  return function(e, r, t) {
    var i = Object(e);
    if (!D(e)) {
      var a = $(r);
      e = T(e), r = function(u) {
        return a(i[u], u, i);
      };
    }
    var o = n(e, r, t);
    return o > -1 ? i[a ? e[o] : o] : void 0;
  };
}
var xo = Math.max;
function To(n, e, r) {
  var t = n == null ? 0 : n.length;
  if (!t)
    return -1;
  var i = r == null ? 0 : nt(r);
  return i < 0 && (i = xo(t + i, 0)), Ve(n, $(e), i);
}
var Oo = yo(To);
const ne = Oo;
function _r(n, e) {
  var r = -1, t = D(n) ? Array(n.length) : [];
  return xn(n, function(i, a, o) {
    t[++r] = e(i, a, o);
  }), t;
}
function y(n, e) {
  var r = _(n) ? H : _r;
  return r(n, $(e));
}
function Lo(n, e) {
  return n == null ? n : zn(n, Qn(e), B);
}
function Ao(n, e) {
  return n && Jn(n, Qn(e));
}
function Po(n, e) {
  return n > e;
}
var No = Object.prototype, Co = No.hasOwnProperty;
function $o(n, e) {
  return n != null && Co.call(n, e);
}
function b(n, e) {
  return n != null && gr(n, e, $o);
}
function Io(n, e) {
  return H(e, function(r) {
    return n[r];
  });
}
function N(n) {
  return n == null ? [] : Io(n, T(n));
}
function x(n) {
  return n === void 0;
}
function Er(n, e) {
  return n < e;
}
function Tn(n, e) {
  var r = {};
  return e = $(e), Jn(n, function(t, i, a) {
    bn(r, i, e(t, i, a));
  }), r;
}
function ee(n, e, r) {
  for (var t = -1, i = n.length; ++t < i; ) {
    var a = n[t], o = e(a);
    if (o != null && (u === void 0 ? o === o && !R(o) : r(o, u)))
      var u = o, s = a;
  }
  return s;
}
function F(n) {
  return n && n.length ? ee(n, G, Po) : void 0;
}
var So = Et(function(n, e, r) {
  mr(n, e, r);
});
const Gn = So;
function J(n) {
  return n && n.length ? ee(n, G, Er) : void 0;
}
function re(n, e) {
  return n && n.length ? ee(n, $(e), Er) : void 0;
}
function Mo(n, e, r, t) {
  if (!L(n))
    return n;
  e = _n(e, n);
  for (var i = -1, a = e.length, o = a - 1, u = n; u != null && ++i < a; ) {
    var s = rn(e[i]), f = r;
    if (s === "__proto__" || s === "constructor" || s === "prototype")
      return n;
    if (i != o) {
      var d = u[s];
      f = t ? t(d, s, u) : void 0, f === void 0 && (f = L(d) ? d : pn(e[i + 1]) ? [] : {});
    }
    wn(u, s, f), u = u[s];
  }
  return n;
}
function Ro(n, e, r) {
  for (var t = -1, i = e.length, a = {}; ++t < i; ) {
    var o = e[t], u = En(n, o);
    r(u, o) && Mo(a, _n(o, n), u);
  }
  return a;
}
function Fo(n, e) {
  var r = n.length;
  for (n.sort(e); r--; )
    n[r] = n[r].value;
  return n;
}
function Do(n, e) {
  if (n !== e) {
    var r = n !== void 0, t = n === null, i = n === n, a = R(n), o = e !== void 0, u = e === null, s = e === e, f = R(e);
    if (!u && !f && !a && n > e || a && o && s && !u && !f || t && o && s || !r && s || !i)
      return 1;
    if (!t && !a && !f && n < e || f && r && i && !t && !a || u && r && i || !o && i || !s)
      return -1;
  }
  return 0;
}
function Go(n, e, r) {
  for (var t = -1, i = n.criteria, a = e.criteria, o = i.length, u = r.length; ++t < o; ) {
    var s = Do(i[t], a[t]);
    if (s) {
      if (t >= u)
        return s;
      var f = r[t];
      return s * (f == "desc" ? -1 : 1);
    }
  }
  return n.index - e.index;
}
function Bo(n, e, r) {
  e.length ? e = H(e, function(a) {
    return _(a) ? function(o) {
      return En(o, a.length === 1 ? a[0] : a);
    } : a;
  }) : e = [G];
  var t = -1;
  e = H(e, Yn($));
  var i = _r(n, function(a, o, u) {
    var s = H(e, function(f) {
      return f(a);
    });
    return { criteria: s, index: ++t, value: a };
  });
  return Fo(i, function(a, o) {
    return Go(a, o, r);
  });
}
function Uo(n, e) {
  return Ro(n, e, function(r, t) {
    return pr(n, t);
  });
}
var Yo = Ut(function(n, e) {
  return n == null ? {} : Uo(n, e);
});
const gn = Yo;
var Ho = Math.ceil, ko = Math.max;
function qo(n, e, r, t) {
  for (var i = -1, a = ko(Ho((e - n) / (r || 1)), 0), o = Array(a); a--; )
    o[t ? a : ++i] = n, n += r;
  return o;
}
function Ko(n) {
  return function(e, r, t) {
    return t && typeof t != "number" && j(e, r, t) && (r = t = void 0), e = fn(e), r === void 0 ? (r = e, e = 0) : r = fn(r), t = t === void 0 ? e < r ? 1 : -1 : fn(t), qo(e, r, t, n);
  };
}
var Wo = Ko();
const k = Wo;
function Xo(n, e, r, t, i) {
  return i(n, function(a, o, u) {
    r = t ? (t = !1, a) : e(r, a, o, u);
  }), r;
}
function tn(n, e, r) {
  var t = _(n) ? Zt : Xo, i = arguments.length < 3;
  return t(n, $(e), r, i, xn);
}
var Zo = mn(function(n, e) {
  if (n == null)
    return [];
  var r = e.length;
  return r > 1 && j(n, e[0], e[1]) ? e = [] : r > 2 && j(e[0], e[1], e[2]) && (e = [e[0]]), Bo(n, yn(e, 1), []);
});
const an = Zo;
var Vo = 1 / 0, jo = Ln && 1 / Vn(new Ln([, -0]))[1] == Vo ? function(n) {
  return new Ln(n);
} : it;
const zo = jo;
var Jo = 200;
function Qo(n, e, r) {
  var t = -1, i = pt, a = n.length, o = !0, u = [], s = u;
  if (r)
    o = !1, i = _o;
  else if (a >= Jo) {
    var f = e ? null : zo(n);
    if (f)
      return Vn(f);
    o = !1, i = cr, s = new z();
  } else
    s = e ? [] : u;
  n:
    for (; ++t < a; ) {
      var d = n[t], l = e ? e(d) : d;
      if (d = r || d !== 0 ? d : 0, o && l === l) {
        for (var h = s.length; h--; )
          if (s[h] === l)
            continue n;
        e && s.push(l), u.push(d);
      } else
        i(s, l, r) || (s !== u && s.push(l), u.push(d));
    }
  return u;
}
var nu = mn(function(n) {
  return Qo(yn(n, 1, wr, !0));
});
const eu = nu;
var ru = 0;
function te(n) {
  var e = ++ru;
  return Je(n) + e;
}
function tu(n, e, r) {
  for (var t = -1, i = n.length, a = e.length, o = {}; ++t < i; ) {
    var u = t < a ? e[t] : void 0;
    r(o, n[t], u);
  }
  return o;
}
function iu(n, e) {
  return tu(n || [], e || [], wn);
}
var au = "\0", S = "\0", Ne = "";
class A {
  constructor(e = {}) {
    this._isDirected = b(e, "directed") ? e.directed : !0, this._isMultigraph = b(e, "multigraph") ? e.multigraph : !1, this._isCompound = b(e, "compound") ? e.compound : !1, this._label = void 0, this._defaultNodeLabelFn = Y(void 0), this._defaultEdgeLabelFn = Y(void 0), this._nodes = {}, this._isCompound && (this._parent = {}, this._children = {}, this._children[S] = {}), this._in = {}, this._preds = {}, this._out = {}, this._sucs = {}, this._edgeObjs = {}, this._edgeLabels = {};
  }
  /* === Graph functions ========= */
  isDirected() {
    return this._isDirected;
  }
  isMultigraph() {
    return this._isMultigraph;
  }
  isCompound() {
    return this._isCompound;
  }
  setGraph(e) {
    return this._label = e, this;
  }
  graph() {
    return this._label;
  }
  /* === Node functions ========== */
  setDefaultNodeLabel(e) {
    return Mn(e) || (e = Y(e)), this._defaultNodeLabelFn = e, this;
  }
  nodeCount() {
    return this._nodeCount;
  }
  nodes() {
    return T(this._nodes);
  }
  sources() {
    var e = this;
    return P(this.nodes(), function(r) {
      return se(e._in[r]);
    });
  }
  sinks() {
    var e = this;
    return P(this.nodes(), function(r) {
      return se(e._out[r]);
    });
  }
  setNodes(e, r) {
    var t = arguments, i = this;
    return c(e, function(a) {
      t.length > 1 ? i.setNode(a, r) : i.setNode(a);
    }), this;
  }
  setNode(e, r) {
    return b(this._nodes, e) ? (arguments.length > 1 && (this._nodes[e] = r), this) : (this._nodes[e] = arguments.length > 1 ? r : this._defaultNodeLabelFn(e), this._isCompound && (this._parent[e] = S, this._children[e] = {}, this._children[S][e] = !0), this._in[e] = {}, this._preds[e] = {}, this._out[e] = {}, this._sucs[e] = {}, ++this._nodeCount, this);
  }
  node(e) {
    return this._nodes[e];
  }
  hasNode(e) {
    return b(this._nodes, e);
  }
  removeNode(e) {
    var r = this;
    if (b(this._nodes, e)) {
      var t = function(i) {
        r.removeEdge(r._edgeObjs[i]);
      };
      delete this._nodes[e], this._isCompound && (this._removeFromParentsChildList(e), delete this._parent[e], c(this.children(e), function(i) {
        r.setParent(i);
      }), delete this._children[e]), c(T(this._in[e]), t), delete this._in[e], delete this._preds[e], c(T(this._out[e]), t), delete this._out[e], delete this._sucs[e], --this._nodeCount;
    }
    return this;
  }
  setParent(e, r) {
    if (!this._isCompound)
      throw new Error("Cannot set parent in a non-compound graph");
    if (x(r))
      r = S;
    else {
      r += "";
      for (var t = r; !x(t); t = this.parent(t))
        if (t === e)
          throw new Error("Setting " + r + " as parent of " + e + " would create a cycle");
      this.setNode(r);
    }
    return this.setNode(e), this._removeFromParentsChildList(e), this._parent[e] = r, this._children[r][e] = !0, this;
  }
  _removeFromParentsChildList(e) {
    delete this._children[this._parent[e]][e];
  }
  parent(e) {
    if (this._isCompound) {
      var r = this._parent[e];
      if (r !== S)
        return r;
    }
  }
  children(e) {
    if (x(e) && (e = S), this._isCompound) {
      var r = this._children[e];
      if (r)
        return T(r);
    } else {
      if (e === S)
        return this.nodes();
      if (this.hasNode(e))
        return [];
    }
  }
  predecessors(e) {
    var r = this._preds[e];
    if (r)
      return T(r);
  }
  successors(e) {
    var r = this._sucs[e];
    if (r)
      return T(r);
  }
  neighbors(e) {
    var r = this.predecessors(e);
    if (r)
      return eu(r, this.successors(e));
  }
  isLeaf(e) {
    var r;
    return this.isDirected() ? r = this.successors(e) : r = this.neighbors(e), r.length === 0;
  }
  filterNodes(e) {
    var r = new this.constructor({
      directed: this._isDirected,
      multigraph: this._isMultigraph,
      compound: this._isCompound
    });
    r.setGraph(this.graph());
    var t = this;
    c(this._nodes, function(o, u) {
      e(u) && r.setNode(u, o);
    }), c(this._edgeObjs, function(o) {
      r.hasNode(o.v) && r.hasNode(o.w) && r.setEdge(o, t.edge(o));
    });
    var i = {};
    function a(o) {
      var u = t.parent(o);
      return u === void 0 || r.hasNode(u) ? (i[o] = u, u) : u in i ? i[u] : a(u);
    }
    return this._isCompound && c(r.nodes(), function(o) {
      r.setParent(o, a(o));
    }), r;
  }
  /* === Edge functions ========== */
  setDefaultEdgeLabel(e) {
    return Mn(e) || (e = Y(e)), this._defaultEdgeLabelFn = e, this;
  }
  edgeCount() {
    return this._edgeCount;
  }
  edges() {
    return N(this._edgeObjs);
  }
  setPath(e, r) {
    var t = this, i = arguments;
    return tn(e, function(a, o) {
      return i.length > 1 ? t.setEdge(a, o, r) : t.setEdge(a, o), o;
    }), this;
  }
  /*
   * setEdge(v, w, [value, [name]])
   * setEdge({ v, w, [name] }, [value])
   */
  setEdge() {
    var e, r, t, i, a = !1, o = arguments[0];
    typeof o == "object" && o !== null && "v" in o ? (e = o.v, r = o.w, t = o.name, arguments.length === 2 && (i = arguments[1], a = !0)) : (e = o, r = arguments[1], t = arguments[3], arguments.length > 2 && (i = arguments[2], a = !0)), e = "" + e, r = "" + r, x(t) || (t = "" + t);
    var u = W(this._isDirected, e, r, t);
    if (b(this._edgeLabels, u))
      return a && (this._edgeLabels[u] = i), this;
    if (!x(t) && !this._isMultigraph)
      throw new Error("Cannot set a named edge when isMultigraph = false");
    this.setNode(e), this.setNode(r), this._edgeLabels[u] = a ? i : this._defaultEdgeLabelFn(e, r, t);
    var s = ou(this._isDirected, e, r, t);
    return e = s.v, r = s.w, Object.freeze(s), this._edgeObjs[u] = s, Ce(this._preds[r], e), Ce(this._sucs[e], r), this._in[r][u] = s, this._out[e][u] = s, this._edgeCount++, this;
  }
  edge(e, r, t) {
    var i = arguments.length === 1 ? Pn(this._isDirected, arguments[0]) : W(this._isDirected, e, r, t);
    return this._edgeLabels[i];
  }
  hasEdge(e, r, t) {
    var i = arguments.length === 1 ? Pn(this._isDirected, arguments[0]) : W(this._isDirected, e, r, t);
    return b(this._edgeLabels, i);
  }
  removeEdge(e, r, t) {
    var i = arguments.length === 1 ? Pn(this._isDirected, arguments[0]) : W(this._isDirected, e, r, t), a = this._edgeObjs[i];
    return a && (e = a.v, r = a.w, delete this._edgeLabels[i], delete this._edgeObjs[i], $e(this._preds[r], e), $e(this._sucs[e], r), delete this._in[r][i], delete this._out[e][i], this._edgeCount--), this;
  }
  inEdges(e, r) {
    var t = this._in[e];
    if (t) {
      var i = N(t);
      return r ? P(i, function(a) {
        return a.v === r;
      }) : i;
    }
  }
  outEdges(e, r) {
    var t = this._out[e];
    if (t) {
      var i = N(t);
      return r ? P(i, function(a) {
        return a.w === r;
      }) : i;
    }
  }
  nodeEdges(e, r) {
    var t = this.inEdges(e, r);
    if (t)
      return t.concat(this.outEdges(e, r));
  }
}
A.prototype._nodeCount = 0;
A.prototype._edgeCount = 0;
function Ce(n, e) {
  n[e] ? n[e]++ : n[e] = 1;
}
function $e(n, e) {
  --n[e] || delete n[e];
}
function W(n, e, r, t) {
  var i = "" + e, a = "" + r;
  if (!n && i > a) {
    var o = i;
    i = a, a = o;
  }
  return i + Ne + a + Ne + (x(t) ? au : t);
}
function ou(n, e, r, t) {
  var i = "" + e, a = "" + r;
  if (!n && i > a) {
    var o = i;
    i = a, a = o;
  }
  var u = { v: i, w: a };
  return t && (u.name = t), u;
}
function Pn(n, e) {
  return W(n, e.v, e.w, e.name);
}
class uu {
  constructor() {
    var e = {};
    e._next = e._prev = e, this._sentinel = e;
  }
  dequeue() {
    var e = this._sentinel, r = e._prev;
    if (r !== e)
      return Ie(r), r;
  }
  enqueue(e) {
    var r = this._sentinel;
    e._prev && e._next && Ie(e), e._next = r._next, r._next._prev = e, r._next = e, e._prev = r;
  }
  toString() {
    for (var e = [], r = this._sentinel, t = r._prev; t !== r; )
      e.push(JSON.stringify(t, su)), t = t._prev;
    return "[" + e.join(", ") + "]";
  }
}
function Ie(n) {
  n._prev._next = n._next, n._next._prev = n._prev, delete n._next, delete n._prev;
}
function su(n, e) {
  if (n !== "_next" && n !== "_prev")
    return e;
}
var fu = Y(1);
function du(n, e) {
  if (n.nodeCount() <= 1)
    return [];
  var r = lu(n, e || fu), t = cu(r.graph, r.buckets, r.zeroIdx);
  return q(
    y(t, function(i) {
      return n.outEdges(i.v, i.w);
    })
  );
}
function cu(n, e, r) {
  for (var t = [], i = e[e.length - 1], a = e[0], o; n.nodeCount(); ) {
    for (; o = a.dequeue(); )
      Nn(n, e, r, o);
    for (; o = i.dequeue(); )
      Nn(n, e, r, o);
    if (n.nodeCount()) {
      for (var u = e.length - 2; u > 0; --u)
        if (o = e[u].dequeue(), o) {
          t = t.concat(Nn(n, e, r, o, !0));
          break;
        }
    }
  }
  return t;
}
function Nn(n, e, r, t, i) {
  var a = i ? [] : void 0;
  return c(n.inEdges(t.v), function(o) {
    var u = n.edge(o), s = n.node(o.v);
    i && a.push({ v: o.v, w: o.w }), s.out -= u, Bn(e, r, s);
  }), c(n.outEdges(t.v), function(o) {
    var u = n.edge(o), s = o.w, f = n.node(s);
    f.in -= u, Bn(e, r, f);
  }), n.removeNode(t.v), a;
}
function lu(n, e) {
  var r = new A(), t = 0, i = 0;
  c(n.nodes(), function(u) {
    r.setNode(u, { v: u, in: 0, out: 0 });
  }), c(n.edges(), function(u) {
    var s = r.edge(u.v, u.w) || 0, f = e(u), d = s + f;
    r.setEdge(u.v, u.w, d), i = Math.max(i, r.node(u.v).out += f), t = Math.max(t, r.node(u.w).in += f);
  });
  var a = k(i + t + 3).map(function() {
    return new uu();
  }), o = t + 1;
  return c(r.nodes(), function(u) {
    Bn(a, o, r.node(u));
  }), { graph: r, buckets: a, zeroIdx: o };
}
function Bn(n, e, r) {
  r.out ? r.in ? n[r.out - r.in + e].enqueue(r) : n[n.length - 1].enqueue(r) : n[0].enqueue(r);
}
function hu(n) {
  var e = n.graph().acyclicer === "greedy" ? du(n, r(n)) : vu(n);
  c(e, function(t) {
    var i = n.edge(t);
    n.removeEdge(t), i.forwardName = t.name, i.reversed = !0, n.setEdge(t.w, t.v, i, te("rev"));
  });
  function r(t) {
    return function(i) {
      return t.edge(i).weight;
    };
  }
}
function vu(n) {
  var e = [], r = {}, t = {};
  function i(a) {
    b(t, a) || (t[a] = !0, r[a] = !0, c(n.outEdges(a), function(o) {
      b(r, o.w) ? e.push(o) : i(o.w);
    }), delete r[a]);
  }
  return c(n.nodes(), i), e;
}
function gu(n) {
  c(n.edges(), function(e) {
    var r = n.edge(e);
    if (r.reversed) {
      n.removeEdge(e);
      var t = r.forwardName;
      delete r.reversed, delete r.forwardName, n.setEdge(e.w, e.v, r, t);
    }
  });
}
function K(n, e, r, t) {
  var i;
  do
    i = te(t);
  while (n.hasNode(i));
  return r.dummy = e, n.setNode(i, r), i;
}
function pu(n) {
  var e = new A().setGraph(n.graph());
  return c(n.nodes(), function(r) {
    e.setNode(r, n.node(r));
  }), c(n.edges(), function(r) {
    var t = e.edge(r.v, r.w) || { weight: 0, minlen: 1 }, i = n.edge(r);
    e.setEdge(r.v, r.w, {
      weight: t.weight + i.weight,
      minlen: Math.max(t.minlen, i.minlen)
    });
  }), e;
}
function yr(n) {
  var e = new A({ multigraph: n.isMultigraph() }).setGraph(n.graph());
  return c(n.nodes(), function(r) {
    n.children(r).length || e.setNode(r, n.node(r));
  }), c(n.edges(), function(r) {
    e.setEdge(r, n.edge(r));
  }), e;
}
function Se(n, e) {
  var r = n.x, t = n.y, i = e.x - r, a = e.y - t, o = n.width / 2, u = n.height / 2;
  if (!i && !a)
    throw new Error("Not possible to find intersection inside of the rectangle");
  var s, f;
  return Math.abs(a) * o > Math.abs(i) * u ? (a < 0 && (u = -u), s = u * i / a, f = u) : (i < 0 && (o = -o), s = o, f = o * a / i), { x: r + s, y: t + f };
}
function On(n) {
  var e = y(k(xr(n) + 1), function() {
    return [];
  });
  return c(n.nodes(), function(r) {
    var t = n.node(r), i = t.rank;
    x(i) || (e[i][t.order] = r);
  }), e;
}
function bu(n) {
  var e = J(
    y(n.nodes(), function(r) {
      return n.node(r).rank;
    })
  );
  c(n.nodes(), function(r) {
    var t = n.node(r);
    b(t, "rank") && (t.rank -= e);
  });
}
function wu(n) {
  var e = J(
    y(n.nodes(), function(a) {
      return n.node(a).rank;
    })
  ), r = [];
  c(n.nodes(), function(a) {
    var o = n.node(a).rank - e;
    r[o] || (r[o] = []), r[o].push(a);
  });
  var t = 0, i = n.graph().nodeRankFactor;
  c(r, function(a, o) {
    x(a) && o % i !== 0 ? --t : t && c(a, function(u) {
      n.node(u).rank += t;
    });
  });
}
function Me(n, e, r, t) {
  var i = {
    width: 0,
    height: 0
  };
  return arguments.length >= 4 && (i.rank = r, i.order = t), K(n, "border", i, e);
}
function xr(n) {
  return F(
    y(n.nodes(), function(e) {
      var r = n.node(e).rank;
      if (!x(r))
        return r;
    })
  );
}
function mu(n, e) {
  var r = { lhs: [], rhs: [] };
  return c(n, function(t) {
    e(t) ? r.lhs.push(t) : r.rhs.push(t);
  }), r;
}
function _u(n, e) {
  var r = Pe();
  try {
    return e();
  } finally {
    console.log(n + " time: " + (Pe() - r) + "ms");
  }
}
function Eu(n, e) {
  return e();
}
function yu(n) {
  function e(r) {
    var t = n.children(r), i = n.node(r);
    if (t.length && c(t, e), b(i, "minRank")) {
      i.borderLeft = [], i.borderRight = [];
      for (var a = i.minRank, o = i.maxRank + 1; a < o; ++a)
        Re(n, "borderLeft", "_bl", r, i, a), Re(n, "borderRight", "_br", r, i, a);
    }
  }
  c(n.children(), e);
}
function Re(n, e, r, t, i, a) {
  var o = { width: 0, height: 0, rank: a, borderType: e }, u = i[e][a - 1], s = K(n, "border", o, r);
  i[e][a] = s, n.setParent(s, t), u && n.setEdge(u, s, { weight: 1 });
}
function xu(n) {
  var e = n.graph().rankdir.toLowerCase();
  (e === "lr" || e === "rl") && Tr(n);
}
function Tu(n) {
  var e = n.graph().rankdir.toLowerCase();
  (e === "bt" || e === "rl") && Ou(n), (e === "lr" || e === "rl") && (Lu(n), Tr(n));
}
function Tr(n) {
  c(n.nodes(), function(e) {
    Fe(n.node(e));
  }), c(n.edges(), function(e) {
    Fe(n.edge(e));
  });
}
function Fe(n) {
  var e = n.width;
  n.width = n.height, n.height = e;
}
function Ou(n) {
  c(n.nodes(), function(e) {
    Cn(n.node(e));
  }), c(n.edges(), function(e) {
    var r = n.edge(e);
    c(r.points, Cn), b(r, "y") && Cn(r);
  });
}
function Cn(n) {
  n.y = -n.y;
}
function Lu(n) {
  c(n.nodes(), function(e) {
    $n(n.node(e));
  }), c(n.edges(), function(e) {
    var r = n.edge(e);
    c(r.points, $n), b(r, "x") && $n(r);
  });
}
function $n(n) {
  var e = n.x;
  n.x = n.y, n.y = e;
}
function Au(n) {
  n.graph().dummyChains = [], c(n.edges(), function(e) {
    Pu(n, e);
  });
}
function Pu(n, e) {
  var r = e.v, t = n.node(r).rank, i = e.w, a = n.node(i).rank, o = e.name, u = n.edge(e), s = u.labelRank;
  if (a !== t + 1) {
    n.removeEdge(e);
    var f, d, l;
    for (l = 0, ++t; t < a; ++l, ++t)
      u.points = [], d = {
        width: 0,
        height: 0,
        edgeLabel: u,
        edgeObj: e,
        rank: t
      }, f = K(n, "edge", d, "_d"), t === s && (d.width = u.width, d.height = u.height, d.dummy = "edge-label", d.labelpos = u.labelpos), n.setEdge(r, f, { weight: u.weight }, o), l === 0 && n.graph().dummyChains.push(f), r = f;
    n.setEdge(r, i, { weight: u.weight }, o);
  }
}
function Nu(n) {
  c(n.graph().dummyChains, function(e) {
    var r = n.node(e), t = r.edgeLabel, i;
    for (n.setEdge(r.edgeObj, t); r.dummy; )
      i = n.successors(e)[0], n.removeNode(e), t.points.push({ x: r.x, y: r.y }), r.dummy === "edge-label" && (t.x = r.x, t.y = r.y, t.width = r.width, t.height = r.height), e = i, r = n.node(e);
  });
}
function ie(n) {
  var e = {};
  function r(t) {
    var i = n.node(t);
    if (b(e, t))
      return i.rank;
    e[t] = !0;
    var a = J(
      y(n.outEdges(t), function(o) {
        return r(o.w) - n.edge(o).minlen;
      })
    );
    return (a === Number.POSITIVE_INFINITY || // return value of _.map([]) for Lodash 3
    a === void 0 || // return value of _.map([]) for Lodash 4
    a === null) && (a = 0), i.rank = a;
  }
  c(n.sources(), r);
}
function Q(n, e) {
  return n.node(e.w).rank - n.node(e.v).rank - n.edge(e).minlen;
}
function Or(n) {
  var e = new A({ directed: !1 }), r = n.nodes()[0], t = n.nodeCount();
  e.setNode(r, {});
  for (var i, a; Cu(e, n) < t; )
    i = $u(e, n), a = e.hasNode(i.v) ? Q(n, i) : -Q(n, i), Iu(e, n, a);
  return e;
}
function Cu(n, e) {
  function r(t) {
    c(e.nodeEdges(t), function(i) {
      var a = i.v, o = t === a ? i.w : a;
      !n.hasNode(o) && !Q(e, i) && (n.setNode(o, {}), n.setEdge(t, o, {}), r(o));
    });
  }
  return c(n.nodes(), r), n.nodeCount();
}
function $u(n, e) {
  return re(e.edges(), function(r) {
    if (n.hasNode(r.v) !== n.hasNode(r.w))
      return Q(e, r);
  });
}
function Iu(n, e, r) {
  c(n.nodes(), function(t) {
    e.node(t).rank += r;
  });
}
function Su() {
}
Su.prototype = new Error();
function Lr(n, e, r) {
  _(e) || (e = [e]);
  var t = (n.isDirected() ? n.successors : n.neighbors).bind(n), i = [], a = {};
  return c(e, function(o) {
    if (!n.hasNode(o))
      throw new Error("Graph does not have node: " + o);
    Ar(n, o, r === "post", a, t, i);
  }), i;
}
function Ar(n, e, r, t, i, a) {
  b(t, e) || (t[e] = !0, r || a.push(e), c(i(e), function(o) {
    Ar(n, o, r, t, i, a);
  }), r && a.push(e));
}
function Mu(n, e) {
  return Lr(n, e, "post");
}
function Ru(n, e) {
  return Lr(n, e, "pre");
}
U.initLowLimValues = oe;
U.initCutValues = ae;
U.calcCutValue = Pr;
U.leaveEdge = Cr;
U.enterEdge = $r;
U.exchangeEdges = Ir;
function U(n) {
  n = pu(n), ie(n);
  var e = Or(n);
  oe(e), ae(e, n);
  for (var r, t; r = Cr(e); )
    t = $r(e, n, r), Ir(e, n, r, t);
}
function ae(n, e) {
  var r = Mu(n, n.nodes());
  r = r.slice(0, r.length - 1), c(r, function(t) {
    Fu(n, e, t);
  });
}
function Fu(n, e, r) {
  var t = n.node(r), i = t.parent;
  n.edge(r, i).cutvalue = Pr(n, e, r);
}
function Pr(n, e, r) {
  var t = n.node(r), i = t.parent, a = !0, o = e.edge(r, i), u = 0;
  return o || (a = !1, o = e.edge(i, r)), u = o.weight, c(e.nodeEdges(r), function(s) {
    var f = s.v === r, d = f ? s.w : s.v;
    if (d !== i) {
      var l = f === a, h = e.edge(s).weight;
      if (u += l ? h : -h, Gu(n, r, d)) {
        var g = n.edge(r, d).cutvalue;
        u += l ? -g : g;
      }
    }
  }), u;
}
function oe(n, e) {
  arguments.length < 2 && (e = n.nodes()[0]), Nr(n, {}, 1, e);
}
function Nr(n, e, r, t, i) {
  var a = r, o = n.node(t);
  return e[t] = !0, c(n.neighbors(t), function(u) {
    b(e, u) || (r = Nr(n, e, r, u, t));
  }), o.low = a, o.lim = r++, i ? o.parent = i : delete o.parent, r;
}
function Cr(n) {
  return ne(n.edges(), function(e) {
    return n.edge(e).cutvalue < 0;
  });
}
function $r(n, e, r) {
  var t = r.v, i = r.w;
  e.hasEdge(t, i) || (t = r.w, i = r.v);
  var a = n.node(t), o = n.node(i), u = a, s = !1;
  a.lim > o.lim && (u = o, s = !0);
  var f = P(e.edges(), function(d) {
    return s === De(n, n.node(d.v), u) && s !== De(n, n.node(d.w), u);
  });
  return re(f, function(d) {
    return Q(e, d);
  });
}
function Ir(n, e, r, t) {
  var i = r.v, a = r.w;
  n.removeEdge(i, a), n.setEdge(t.v, t.w, {}), oe(n), ae(n, e), Du(n, e);
}
function Du(n, e) {
  var r = ne(n.nodes(), function(i) {
    return !e.node(i).parent;
  }), t = Ru(n, r);
  t = t.slice(1), c(t, function(i) {
    var a = n.node(i).parent, o = e.edge(i, a), u = !1;
    o || (o = e.edge(a, i), u = !0), e.node(i).rank = e.node(a).rank + (u ? o.minlen : -o.minlen);
  });
}
function Gu(n, e, r) {
  return n.hasEdge(e, r);
}
function De(n, e, r) {
  return r.low <= e.lim && e.lim <= r.lim;
}
function Bu(n) {
  switch (n.graph().ranker) {
    case "network-simplex":
      Ge(n);
      break;
    case "tight-tree":
      Yu(n);
      break;
    case "longest-path":
      Uu(n);
      break;
    default:
      Ge(n);
  }
}
var Uu = ie;
function Yu(n) {
  ie(n), Or(n);
}
function Ge(n) {
  U(n);
}
function Hu(n) {
  var e = K(n, "root", {}, "_root"), r = ku(n), t = F(N(r)) - 1, i = 2 * t + 1;
  n.graph().nestingRoot = e, c(n.edges(), function(o) {
    n.edge(o).minlen *= i;
  });
  var a = qu(n) + 1;
  c(n.children(), function(o) {
    Sr(n, e, i, a, t, r, o);
  }), n.graph().nodeRankFactor = i;
}
function Sr(n, e, r, t, i, a, o) {
  var u = n.children(o);
  if (!u.length) {
    o !== e && n.setEdge(e, o, { weight: 0, minlen: r });
    return;
  }
  var s = Me(n, "_bt"), f = Me(n, "_bb"), d = n.node(o);
  n.setParent(s, o), d.borderTop = s, n.setParent(f, o), d.borderBottom = f, c(u, function(l) {
    Sr(n, e, r, t, i, a, l);
    var h = n.node(l), g = h.borderTop ? h.borderTop : l, v = h.borderBottom ? h.borderBottom : l, p = h.borderTop ? t : 2 * t, m = g !== v ? 1 : i - a[o] + 1;
    n.setEdge(s, g, {
      weight: p,
      minlen: m,
      nestingEdge: !0
    }), n.setEdge(v, f, {
      weight: p,
      minlen: m,
      nestingEdge: !0
    });
  }), n.parent(o) || n.setEdge(e, s, { weight: 0, minlen: i + a[o] });
}
function ku(n) {
  var e = {};
  function r(t, i) {
    var a = n.children(t);
    a && a.length && c(a, function(o) {
      r(o, i + 1);
    }), e[t] = i;
  }
  return c(n.children(), function(t) {
    r(t, 1);
  }), e;
}
function qu(n) {
  return tn(
    n.edges(),
    function(e, r) {
      return e + n.edge(r).weight;
    },
    0
  );
}
function Ku(n) {
  var e = n.graph();
  n.removeNode(e.nestingRoot), delete e.nestingRoot, c(n.edges(), function(r) {
    var t = n.edge(r);
    t.nestingEdge && n.removeEdge(r);
  });
}
function Wu(n, e, r) {
  var t = {}, i;
  c(r, function(a) {
    for (var o = n.parent(a), u, s; o; ) {
      if (u = n.parent(o), u ? (s = t[u], t[u] = o) : (s = i, i = o), s && s !== o) {
        e.setEdge(s, o);
        return;
      }
      o = u;
    }
  });
}
function Xu(n, e, r) {
  var t = Zu(n), i = new A({ compound: !0 }).setGraph({ root: t }).setDefaultNodeLabel(function(a) {
    return n.node(a);
  });
  return c(n.nodes(), function(a) {
    var o = n.node(a), u = n.parent(a);
    (o.rank === e || o.minRank <= e && e <= o.maxRank) && (i.setNode(a), i.setParent(a, u || t), c(n[r](a), function(s) {
      var f = s.v === a ? s.w : s.v, d = i.edge(f, a), l = x(d) ? 0 : d.weight;
      i.setEdge(f, a, { weight: n.edge(s).weight + l });
    }), b(o, "minRank") && i.setNode(a, {
      borderLeft: o.borderLeft[e],
      borderRight: o.borderRight[e]
    }));
  }), i;
}
function Zu(n) {
  for (var e; n.hasNode(e = te("_root")); )
    ;
  return e;
}
function Vu(n, e) {
  for (var r = 0, t = 1; t < e.length; ++t)
    r += ju(n, e[t - 1], e[t]);
  return r;
}
function ju(n, e, r) {
  for (var t = iu(
    r,
    y(r, function(f, d) {
      return d;
    })
  ), i = q(
    y(e, function(f) {
      return an(
        y(n.outEdges(f), function(d) {
          return { pos: t[d.w], weight: n.edge(d).weight };
        }),
        "pos"
      );
    })
  ), a = 1; a < r.length; )
    a <<= 1;
  var o = 2 * a - 1;
  a -= 1;
  var u = y(new Array(o), function() {
    return 0;
  }), s = 0;
  return c(
    // @ts-expect-error
    i.forEach(function(f) {
      var d = f.pos + a;
      u[d] += f.weight;
      for (var l = 0; d > 0; )
        d % 2 && (l += u[d + 1]), d = d - 1 >> 1, u[d] += f.weight;
      s += f.weight * l;
    })
  ), s;
}
function zu(n) {
  var e = {}, r = P(n.nodes(), function(u) {
    return !n.children(u).length;
  }), t = F(
    y(r, function(u) {
      return n.node(u).rank;
    })
  ), i = y(k(t + 1), function() {
    return [];
  });
  function a(u) {
    if (!b(e, u)) {
      e[u] = !0;
      var s = n.node(u);
      i[s.rank].push(u), c(n.successors(u), a);
    }
  }
  var o = an(r, function(u) {
    return n.node(u).rank;
  });
  return c(o, a), i;
}
function Ju(n, e) {
  return y(e, function(r) {
    var t = n.inEdges(r);
    if (t.length) {
      var i = tn(
        t,
        function(a, o) {
          var u = n.edge(o), s = n.node(o.v);
          return {
            sum: a.sum + u.weight * s.order,
            weight: a.weight + u.weight
          };
        },
        { sum: 0, weight: 0 }
      );
      return {
        v: r,
        barycenter: i.sum / i.weight,
        weight: i.weight
      };
    } else
      return { v: r };
  });
}
function Qu(n, e) {
  var r = {};
  c(n, function(i, a) {
    var o = r[i.v] = {
      indegree: 0,
      in: [],
      out: [],
      vs: [i.v],
      i: a
    };
    x(i.barycenter) || (o.barycenter = i.barycenter, o.weight = i.weight);
  }), c(e.edges(), function(i) {
    var a = r[i.v], o = r[i.w];
    !x(a) && !x(o) && (o.indegree++, a.out.push(r[i.w]));
  });
  var t = P(r, function(i) {
    return !i.indegree;
  });
  return ns(t);
}
function ns(n) {
  var e = [];
  function r(a) {
    return function(o) {
      o.merged || (x(o.barycenter) || x(a.barycenter) || o.barycenter >= a.barycenter) && es(a, o);
    };
  }
  function t(a) {
    return function(o) {
      o.in.push(a), --o.indegree === 0 && n.push(o);
    };
  }
  for (; n.length; ) {
    var i = n.pop();
    e.push(i), c(i.in.reverse(), r(i)), c(i.out, t(i));
  }
  return y(
    P(e, function(a) {
      return !a.merged;
    }),
    function(a) {
      return gn(a, ["vs", "i", "barycenter", "weight"]);
    }
  );
}
function es(n, e) {
  var r = 0, t = 0;
  n.weight && (r += n.barycenter * n.weight, t += n.weight), e.weight && (r += e.barycenter * e.weight, t += e.weight), n.vs = e.vs.concat(n.vs), n.barycenter = r / t, n.weight = t, n.i = Math.min(e.i, n.i), e.merged = !0;
}
function rs(n, e) {
  var r = mu(n, function(d) {
    return b(d, "barycenter");
  }), t = r.lhs, i = an(r.rhs, function(d) {
    return -d.i;
  }), a = [], o = 0, u = 0, s = 0;
  t.sort(ts(!!e)), s = Be(a, i, s), c(t, function(d) {
    s += d.vs.length, a.push(d.vs), o += d.barycenter * d.weight, u += d.weight, s = Be(a, i, s);
  });
  var f = { vs: q(a) };
  return u && (f.barycenter = o / u, f.weight = u), f;
}
function Be(n, e, r) {
  for (var t; e.length && (t = vn(e)).i <= r; )
    e.pop(), n.push(t.vs), r++;
  return r;
}
function ts(n) {
  return function(e, r) {
    return e.barycenter < r.barycenter ? -1 : e.barycenter > r.barycenter ? 1 : n ? r.i - e.i : e.i - r.i;
  };
}
function Mr(n, e, r, t) {
  var i = n.children(e), a = n.node(e), o = a ? a.borderLeft : void 0, u = a ? a.borderRight : void 0, s = {};
  o && (i = P(i, function(v) {
    return v !== o && v !== u;
  }));
  var f = Ju(n, i);
  c(f, function(v) {
    if (n.children(v.v).length) {
      var p = Mr(n, v.v, r, t);
      s[v.v] = p, b(p, "barycenter") && as(v, p);
    }
  });
  var d = Qu(f, r);
  is(d, s);
  var l = rs(d, t);
  if (o && (l.vs = q([o, l.vs, u]), n.predecessors(o).length)) {
    var h = n.node(n.predecessors(o)[0]), g = n.node(n.predecessors(u)[0]);
    b(l, "barycenter") || (l.barycenter = 0, l.weight = 0), l.barycenter = (l.barycenter * l.weight + h.order + g.order) / (l.weight + 2), l.weight += 2;
  }
  return l;
}
function is(n, e) {
  c(n, function(r) {
    r.vs = q(
      r.vs.map(function(t) {
        return e[t] ? e[t].vs : t;
      })
    );
  });
}
function as(n, e) {
  x(n.barycenter) ? (n.barycenter = e.barycenter, n.weight = e.weight) : (n.barycenter = (n.barycenter * n.weight + e.barycenter * e.weight) / (n.weight + e.weight), n.weight += e.weight);
}
function os(n) {
  var e = xr(n), r = Ue(n, k(1, e + 1), "inEdges"), t = Ue(n, k(e - 1, -1, -1), "outEdges"), i = zu(n);
  Ye(n, i);
  for (var a = Number.POSITIVE_INFINITY, o, u = 0, s = 0; s < 4; ++u, ++s) {
    us(u % 2 ? r : t, u % 4 >= 2), i = On(n);
    var f = Vu(n, i);
    f < a && (s = 0, o = ya(i), a = f);
  }
  Ye(n, o);
}
function Ue(n, e, r) {
  return y(e, function(t) {
    return Xu(n, t, r);
  });
}
function us(n, e) {
  var r = new A();
  c(n, function(t) {
    var i = t.graph().root, a = Mr(t, i, r, e);
    c(a.vs, function(o, u) {
      t.node(o).order = u;
    }), Wu(t, r, a.vs);
  });
}
function Ye(n, e) {
  c(e, function(r) {
    c(r, function(t, i) {
      n.node(t).order = i;
    });
  });
}
function ss(n) {
  var e = ds(n);
  c(n.graph().dummyChains, function(r) {
    for (var t = n.node(r), i = t.edgeObj, a = fs(n, e, i.v, i.w), o = a.path, u = a.lca, s = 0, f = o[s], d = !0; r !== i.w; ) {
      if (t = n.node(r), d) {
        for (; (f = o[s]) !== u && n.node(f).maxRank < t.rank; )
          s++;
        f === u && (d = !1);
      }
      if (!d) {
        for (; s < o.length - 1 && n.node(f = o[s + 1]).minRank <= t.rank; )
          s++;
        f = o[s];
      }
      n.setParent(r, f), r = n.successors(r)[0];
    }
  });
}
function fs(n, e, r, t) {
  var i = [], a = [], o = Math.min(e[r].low, e[t].low), u = Math.max(e[r].lim, e[t].lim), s, f;
  s = r;
  do
    s = n.parent(s), i.push(s);
  while (s && (e[s].low > o || u > e[s].lim));
  for (f = s, s = t; (s = n.parent(s)) !== f; )
    a.push(s);
  return { path: i.concat(a.reverse()), lca: f };
}
function ds(n) {
  var e = {}, r = 0;
  function t(i) {
    var a = r;
    c(n.children(i), t), e[i] = { low: a, lim: r++ };
  }
  return c(n.children(), t), e;
}
function cs(n, e) {
  var r = {};
  function t(i, a) {
    var o = 0, u = 0, s = i.length, f = vn(a);
    return c(a, function(d, l) {
      var h = hs(n, d), g = h ? n.node(h).order : s;
      (h || d === f) && (c(a.slice(u, l + 1), function(v) {
        c(n.predecessors(v), function(p) {
          var m = n.node(p), E = m.order;
          (E < o || g < E) && !(m.dummy && n.node(v).dummy) && Rr(r, p, v);
        });
      }), u = l + 1, o = g);
    }), a;
  }
  return tn(e, t), r;
}
function ls(n, e) {
  var r = {};
  function t(a, o, u, s, f) {
    var d;
    c(k(o, u), function(l) {
      d = a[l], n.node(d).dummy && c(n.predecessors(d), function(h) {
        var g = n.node(h);
        g.dummy && (g.order < s || g.order > f) && Rr(r, h, d);
      });
    });
  }
  function i(a, o) {
    var u = -1, s, f = 0;
    return c(o, function(d, l) {
      if (n.node(d).dummy === "border") {
        var h = n.predecessors(d);
        h.length && (s = n.node(h[0]).order, t(o, f, l, u, s), f = l, u = s);
      }
      t(o, f, o.length, s, a.length);
    }), o;
  }
  return tn(e, i), r;
}
function hs(n, e) {
  if (n.node(e).dummy)
    return ne(n.predecessors(e), function(r) {
      return n.node(r).dummy;
    });
}
function Rr(n, e, r) {
  if (e > r) {
    var t = e;
    e = r, r = t;
  }
  var i = n[e];
  i || (n[e] = i = {}), i[r] = !0;
}
function vs(n, e, r) {
  if (e > r) {
    var t = e;
    e = r, r = t;
  }
  return b(n[e], r);
}
function gs(n, e, r, t) {
  var i = {}, a = {}, o = {};
  return c(e, function(u) {
    c(u, function(s, f) {
      i[s] = s, a[s] = s, o[s] = f;
    });
  }), c(e, function(u) {
    var s = -1;
    c(u, function(f) {
      var d = t(f);
      if (d.length) {
        d = an(d, function(p) {
          return o[p];
        });
        for (var l = (d.length - 1) / 2, h = Math.floor(l), g = Math.ceil(l); h <= g; ++h) {
          var v = d[h];
          a[f] === f && s < o[v] && !vs(r, f, v) && (a[v] = f, a[f] = i[f] = i[v], s = o[v]);
        }
      }
    });
  }), { root: i, align: a };
}
function ps(n, e, r, t, i) {
  var a = {}, o = bs(n, e, r, i), u = i ? "borderLeft" : "borderRight";
  function s(l, h) {
    for (var g = o.nodes(), v = g.pop(), p = {}; v; )
      p[v] ? l(v) : (p[v] = !0, g.push(v), g = g.concat(h(v))), v = g.pop();
  }
  function f(l) {
    a[l] = o.inEdges(l).reduce(function(h, g) {
      return Math.max(h, a[g.v] + o.edge(g));
    }, 0);
  }
  function d(l) {
    var h = o.outEdges(l).reduce(function(v, p) {
      return Math.min(v, a[p.w] - o.edge(p));
    }, Number.POSITIVE_INFINITY), g = n.node(l);
    h !== Number.POSITIVE_INFINITY && g.borderType !== u && (a[l] = Math.max(a[l], h));
  }
  return s(f, o.predecessors.bind(o)), s(d, o.successors.bind(o)), c(t, function(l) {
    a[l] = a[r[l]];
  }), a;
}
function bs(n, e, r, t) {
  var i = new A(), a = n.graph(), o = ys(a.nodesep, a.edgesep, t);
  return c(e, function(u) {
    var s;
    c(u, function(f) {
      var d = r[f];
      if (i.setNode(d), s) {
        var l = r[s], h = i.edge(l, d);
        i.setEdge(l, d, Math.max(o(n, f, s), h || 0));
      }
      s = f;
    });
  }), i;
}
function ws(n, e) {
  return re(N(e), function(r) {
    var t = Number.NEGATIVE_INFINITY, i = Number.POSITIVE_INFINITY;
    return Lo(r, function(a, o) {
      var u = xs(n, o) / 2;
      t = Math.max(a + u, t), i = Math.min(a - u, i);
    }), t - i;
  });
}
function ms(n, e) {
  var r = N(e), t = J(r), i = F(r);
  c(["u", "d"], function(a) {
    c(["l", "r"], function(o) {
      var u = a + o, s = n[u], f;
      if (s !== e) {
        var d = N(s);
        f = o === "l" ? t - J(d) : i - F(d), f && (n[u] = Tn(s, function(l) {
          return l + f;
        }));
      }
    });
  });
}
function _s(n, e) {
  return Tn(n.ul, function(r, t) {
    if (e)
      return n[e.toLowerCase()][t];
    var i = an(y(n, t));
    return (i[1] + i[2]) / 2;
  });
}
function Es(n) {
  var e = On(n), r = Gn(cs(n, e), ls(n, e)), t = {}, i;
  c(["u", "d"], function(o) {
    i = o === "u" ? e : N(e).reverse(), c(["l", "r"], function(u) {
      u === "r" && (i = y(i, function(l) {
        return N(l).reverse();
      }));
      var s = (o === "u" ? n.predecessors : n.successors).bind(n), f = gs(n, i, r, s), d = ps(n, i, f.root, f.align, u === "r");
      u === "r" && (d = Tn(d, function(l) {
        return -l;
      })), t[o + u] = d;
    });
  });
  var a = ws(n, t);
  return ms(t, a), _s(t, n.graph().align);
}
function ys(n, e, r) {
  return function(t, i, a) {
    var o = t.node(i), u = t.node(a), s = 0, f;
    if (s += o.width / 2, b(o, "labelpos"))
      switch (o.labelpos.toLowerCase()) {
        case "l":
          f = -o.width / 2;
          break;
        case "r":
          f = o.width / 2;
          break;
      }
    if (f && (s += r ? f : -f), f = 0, s += (o.dummy ? e : n) / 2, s += (u.dummy ? e : n) / 2, s += u.width / 2, b(u, "labelpos"))
      switch (u.labelpos.toLowerCase()) {
        case "l":
          f = u.width / 2;
          break;
        case "r":
          f = -u.width / 2;
          break;
      }
    return f && (s += r ? f : -f), f = 0, s;
  };
}
function xs(n, e) {
  return n.node(e).width;
}
function Ts(n) {
  n = yr(n), Os(n), Ao(Es(n), function(e, r) {
    n.node(r).x = e;
  });
}
function Os(n) {
  var e = On(n), r = n.graph().ranksep, t = 0;
  c(e, function(i) {
    var a = F(
      y(i, function(o) {
        return n.node(o).height;
      })
    );
    c(i, function(o) {
      n.node(o).y = t + a / 2;
    }), t += a + r;
  });
}
function zs(n, e) {
  var r = e && e.debugTiming ? _u : Eu;
  r("layout", function() {
    var t = r("  buildLayoutGraph", function() {
      return Fs(n);
    });
    r("  runLayout", function() {
      Ls(t, r);
    }), r("  updateInputGraph", function() {
      As(n, t);
    });
  });
}
function Ls(n, e) {
  e("    makeSpaceForEdgeLabels", function() {
    Ds(n);
  }), e("    removeSelfEdges", function() {
    Ws(n);
  }), e("    acyclic", function() {
    hu(n);
  }), e("    nestingGraph.run", function() {
    Hu(n);
  }), e("    rank", function() {
    Bu(yr(n));
  }), e("    injectEdgeLabelProxies", function() {
    Gs(n);
  }), e("    removeEmptyRanks", function() {
    wu(n);
  }), e("    nestingGraph.cleanup", function() {
    Ku(n);
  }), e("    normalizeRanks", function() {
    bu(n);
  }), e("    assignRankMinMax", function() {
    Bs(n);
  }), e("    removeEdgeLabelProxies", function() {
    Us(n);
  }), e("    normalize.run", function() {
    Au(n);
  }), e("    parentDummyChains", function() {
    ss(n);
  }), e("    addBorderSegments", function() {
    yu(n);
  }), e("    order", function() {
    os(n);
  }), e("    insertSelfEdges", function() {
    Xs(n);
  }), e("    adjustCoordinateSystem", function() {
    xu(n);
  }), e("    position", function() {
    Ts(n);
  }), e("    positionSelfEdges", function() {
    Zs(n);
  }), e("    removeBorderNodes", function() {
    Ks(n);
  }), e("    normalize.undo", function() {
    Nu(n);
  }), e("    fixupEdgeLabelCoords", function() {
    ks(n);
  }), e("    undoCoordinateSystem", function() {
    Tu(n);
  }), e("    translateGraph", function() {
    Ys(n);
  }), e("    assignNodeIntersects", function() {
    Hs(n);
  }), e("    reversePoints", function() {
    qs(n);
  }), e("    acyclic.undo", function() {
    gu(n);
  });
}
function As(n, e) {
  c(n.nodes(), function(r) {
    var t = n.node(r), i = e.node(r);
    t && (t.x = i.x, t.y = i.y, e.children(r).length && (t.width = i.width, t.height = i.height));
  }), c(n.edges(), function(r) {
    var t = n.edge(r), i = e.edge(r);
    t.points = i.points, b(i, "x") && (t.x = i.x, t.y = i.y);
  }), n.graph().width = e.graph().width, n.graph().height = e.graph().height;
}
var Ps = ["nodesep", "edgesep", "ranksep", "marginx", "marginy"], Ns = { ranksep: 50, edgesep: 20, nodesep: 50, rankdir: "tb" }, Cs = ["acyclicer", "ranker", "rankdir", "align"], $s = ["width", "height"], Is = { width: 0, height: 0 }, Ss = ["minlen", "weight", "width", "height", "labeloffset"], Ms = {
  minlen: 1,
  weight: 1,
  width: 0,
  height: 0,
  labeloffset: 10,
  labelpos: "r"
}, Rs = ["labelpos"];
function Fs(n) {
  var e = new A({ multigraph: !0, compound: !0 }), r = Sn(n.graph());
  return e.setGraph(
    Gn({}, Ns, In(r, Ps), gn(r, Cs))
  ), c(n.nodes(), function(t) {
    var i = Sn(n.node(t));
    e.setNode(t, bo(In(i, $s), Is)), e.setParent(t, n.parent(t));
  }), c(n.edges(), function(t) {
    var i = Sn(n.edge(t));
    e.setEdge(
      t,
      Gn({}, Ms, In(i, Ss), gn(i, Rs))
    );
  }), e;
}
function Ds(n) {
  var e = n.graph();
  e.ranksep /= 2, c(n.edges(), function(r) {
    var t = n.edge(r);
    t.minlen *= 2, t.labelpos.toLowerCase() !== "c" && (e.rankdir === "TB" || e.rankdir === "BT" ? t.width += t.labeloffset : t.height += t.labeloffset);
  });
}
function Gs(n) {
  c(n.edges(), function(e) {
    var r = n.edge(e);
    if (r.width && r.height) {
      var t = n.node(e.v), i = n.node(e.w), a = { rank: (i.rank - t.rank) / 2 + t.rank, e };
      K(n, "edge-proxy", a, "_ep");
    }
  });
}
function Bs(n) {
  var e = 0;
  c(n.nodes(), function(r) {
    var t = n.node(r);
    t.borderTop && (t.minRank = n.node(t.borderTop).rank, t.maxRank = n.node(t.borderBottom).rank, e = F(e, t.maxRank));
  }), n.graph().maxRank = e;
}
function Us(n) {
  c(n.nodes(), function(e) {
    var r = n.node(e);
    r.dummy === "edge-proxy" && (n.edge(r.e).labelRank = r.rank, n.removeNode(e));
  });
}
function Ys(n) {
  var e = Number.POSITIVE_INFINITY, r = 0, t = Number.POSITIVE_INFINITY, i = 0, a = n.graph(), o = a.marginx || 0, u = a.marginy || 0;
  function s(f) {
    var d = f.x, l = f.y, h = f.width, g = f.height;
    e = Math.min(e, d - h / 2), r = Math.max(r, d + h / 2), t = Math.min(t, l - g / 2), i = Math.max(i, l + g / 2);
  }
  c(n.nodes(), function(f) {
    s(n.node(f));
  }), c(n.edges(), function(f) {
    var d = n.edge(f);
    b(d, "x") && s(d);
  }), e -= o, t -= u, c(n.nodes(), function(f) {
    var d = n.node(f);
    d.x -= e, d.y -= t;
  }), c(n.edges(), function(f) {
    var d = n.edge(f);
    c(d.points, function(l) {
      l.x -= e, l.y -= t;
    }), b(d, "x") && (d.x -= e), b(d, "y") && (d.y -= t);
  }), a.width = r - e + o, a.height = i - t + u;
}
function Hs(n) {
  c(n.edges(), function(e) {
    var r = n.edge(e), t = n.node(e.v), i = n.node(e.w), a, o;
    r.points ? (a = r.points[0], o = r.points[r.points.length - 1]) : (r.points = [], a = i, o = t), r.points.unshift(Se(t, a)), r.points.push(Se(i, o));
  });
}
function ks(n) {
  c(n.edges(), function(e) {
    var r = n.edge(e);
    if (b(r, "x"))
      switch ((r.labelpos === "l" || r.labelpos === "r") && (r.width -= r.labeloffset), r.labelpos) {
        case "l":
          r.x -= r.width / 2 + r.labeloffset;
          break;
        case "r":
          r.x += r.width / 2 + r.labeloffset;
          break;
      }
  });
}
function qs(n) {
  c(n.edges(), function(e) {
    var r = n.edge(e);
    r.reversed && r.points.reverse();
  });
}
function Ks(n) {
  c(n.nodes(), function(e) {
    if (n.children(e).length) {
      var r = n.node(e), t = n.node(r.borderTop), i = n.node(r.borderBottom), a = n.node(vn(r.borderLeft)), o = n.node(vn(r.borderRight));
      r.width = Math.abs(o.x - a.x), r.height = Math.abs(i.y - t.y), r.x = a.x + r.width / 2, r.y = t.y + r.height / 2;
    }
  }), c(n.nodes(), function(e) {
    n.node(e).dummy === "border" && n.removeNode(e);
  });
}
function Ws(n) {
  c(n.edges(), function(e) {
    if (e.v === e.w) {
      var r = n.node(e.v);
      r.selfEdges || (r.selfEdges = []), r.selfEdges.push({ e, label: n.edge(e) }), n.removeEdge(e);
    }
  });
}
function Xs(n) {
  var e = On(n);
  c(e, function(r) {
    var t = 0;
    c(r, function(i, a) {
      var o = n.node(i);
      o.order = a + t, c(o.selfEdges, function(u) {
        K(
          n,
          "selfedge",
          {
            width: u.label.width,
            height: u.label.height,
            rank: o.rank,
            order: a + ++t,
            e: u.e,
            label: u.label
          },
          "_se"
        );
      }), delete o.selfEdges;
    });
  });
}
function Zs(n) {
  c(n.nodes(), function(e) {
    var r = n.node(e);
    if (r.dummy === "selfedge") {
      var t = n.node(r.e.v), i = t.x + t.width / 2, a = t.y, o = r.x - i, u = t.height / 2;
      n.setEdge(r.e, r.label), n.removeNode(e), r.label.points = [
        { x: i + 2 * o / 3, y: a - u },
        { x: i + 5 * o / 6, y: a - u },
        { x: i + o, y: a },
        { x: i + 5 * o / 6, y: a + u },
        { x: i + 2 * o / 3, y: a + u }
      ], r.label.x = r.x, r.label.y = r.y;
    }
  });
}
function In(n, e) {
  return Tn(gn(n, e), Number);
}
function Sn(n) {
  var e = {};
  return c(n, function(r, t) {
    e[t.toLowerCase()] = r;
  }), e;
}
export {
  A as G,
  x as a,
  dn as b,
  bo as d,
  c as f,
  b as h,
  Xt as i,
  zs as l,
  y as m,
  gn as p,
  k as r,
  te as u
};
//# sourceMappingURL=layout-492ec81d.js.map
