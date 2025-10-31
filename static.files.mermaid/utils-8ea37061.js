import { l as x, a as Se, s as Te, g as ke, b as Ee, c as Ne, d as Hi, e as Oe, f as xi, h as Fe } from "./commonDb-41f8b4c5.js";
var gi = {};
Object.defineProperty(gi, "__esModule", { value: !0 });
var Gi = gi.sanitizeUrl = void 0, $e = /^([^\w]*)(javascript|data|vbscript)/im, Me = /&#(\w+)(^\w|;)?/g, Ie = /[\u0000-\u001F\u007F-\u009F\u2000-\u200D\uFEFF]/gim, Pe = /^([^:]+):/gm, je = [".", "/"];
function Le(i) {
  return je.indexOf(i[0]) > -1;
}
function De(i) {
  return i.replace(Me, function(e, n) {
    return String.fromCharCode(n);
  });
}
function Re(i) {
  var e = De(i || "").replace(Ie, "").trim();
  if (!e)
    return "about:blank";
  if (Le(e))
    return e;
  var n = e.match(Pe);
  if (!n)
    return e;
  var t = n[0];
  return $e.test(t) ? "about:blank" : e;
}
Gi = gi.sanitizeUrl = Re;
const Vo = Math.abs, Xo = Math.atan2, Jo = Math.cos, Zo = Math.max, Qo = Math.min, is = Math.sin, es = Math.sqrt, Ni = 1e-12, mi = Math.PI, Oi = mi / 2, ns = 2 * mi;
function ts(i) {
  return i > 1 ? 0 : i < -1 ? mi : Math.acos(i);
}
function rs(i) {
  return i >= 1 ? Oi : i <= -1 ? -Oi : Math.asin(i);
}
function qi(i) {
  this._context = i;
}
qi.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._point = 0;
  },
  lineEnd: function() {
    (this._line || this._line !== 0 && this._point === 1) && this._context.closePath(), this._line = 1 - this._line;
  },
  point: function(i, e) {
    switch (i = +i, e = +e, this._point) {
      case 0:
        this._point = 1, this._line ? this._context.lineTo(i, e) : this._context.moveTo(i, e);
        break;
      case 1:
        this._point = 2;
      default:
        this._context.lineTo(i, e);
        break;
    }
  }
};
function ze(i) {
  return new qi(i);
}
class Ki {
  constructor(e, n) {
    this._context = e, this._x = n;
  }
  areaStart() {
    this._line = 0;
  }
  areaEnd() {
    this._line = NaN;
  }
  lineStart() {
    this._point = 0;
  }
  lineEnd() {
    (this._line || this._line !== 0 && this._point === 1) && this._context.closePath(), this._line = 1 - this._line;
  }
  point(e, n) {
    switch (e = +e, n = +n, this._point) {
      case 0: {
        this._point = 1, this._line ? this._context.lineTo(e, n) : this._context.moveTo(e, n);
        break;
      }
      case 1:
        this._point = 2;
      default: {
        this._x ? this._context.bezierCurveTo(this._x0 = (this._x0 + e) / 2, this._y0, this._x0, n, e, n) : this._context.bezierCurveTo(this._x0, this._y0 = (this._y0 + n) / 2, e, this._y0, e, n);
        break;
      }
    }
    this._x0 = e, this._y0 = n;
  }
}
function Be(i) {
  return new Ki(i, !0);
}
function Ye(i) {
  return new Ki(i, !1);
}
function E() {
}
function K(i, e, n) {
  i._context.bezierCurveTo(
    (2 * i._x0 + i._x1) / 3,
    (2 * i._y0 + i._y1) / 3,
    (i._x0 + 2 * i._x1) / 3,
    (i._y0 + 2 * i._y1) / 3,
    (i._x0 + 4 * i._x1 + e) / 6,
    (i._y0 + 4 * i._y1 + n) / 6
  );
}
function ri(i) {
  this._context = i;
}
ri.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._x0 = this._x1 = this._y0 = this._y1 = NaN, this._point = 0;
  },
  lineEnd: function() {
    switch (this._point) {
      case 3:
        K(this, this._x1, this._y1);
      case 2:
        this._context.lineTo(this._x1, this._y1);
        break;
    }
    (this._line || this._line !== 0 && this._point === 1) && this._context.closePath(), this._line = 1 - this._line;
  },
  point: function(i, e) {
    switch (i = +i, e = +e, this._point) {
      case 0:
        this._point = 1, this._line ? this._context.lineTo(i, e) : this._context.moveTo(i, e);
        break;
      case 1:
        this._point = 2;
        break;
      case 2:
        this._point = 3, this._context.lineTo((5 * this._x0 + this._x1) / 6, (5 * this._y0 + this._y1) / 6);
      default:
        K(this, i, e);
        break;
    }
    this._x0 = this._x1, this._x1 = i, this._y0 = this._y1, this._y1 = e;
  }
};
function We(i) {
  return new ri(i);
}
function Vi(i) {
  this._context = i;
}
Vi.prototype = {
  areaStart: E,
  areaEnd: E,
  lineStart: function() {
    this._x0 = this._x1 = this._x2 = this._x3 = this._x4 = this._y0 = this._y1 = this._y2 = this._y3 = this._y4 = NaN, this._point = 0;
  },
  lineEnd: function() {
    switch (this._point) {
      case 1: {
        this._context.moveTo(this._x2, this._y2), this._context.closePath();
        break;
      }
      case 2: {
        this._context.moveTo((this._x2 + 2 * this._x3) / 3, (this._y2 + 2 * this._y3) / 3), this._context.lineTo((this._x3 + 2 * this._x2) / 3, (this._y3 + 2 * this._y2) / 3), this._context.closePath();
        break;
      }
      case 3: {
        this.point(this._x2, this._y2), this.point(this._x3, this._y3), this.point(this._x4, this._y4);
        break;
      }
    }
  },
  point: function(i, e) {
    switch (i = +i, e = +e, this._point) {
      case 0:
        this._point = 1, this._x2 = i, this._y2 = e;
        break;
      case 1:
        this._point = 2, this._x3 = i, this._y3 = e;
        break;
      case 2:
        this._point = 3, this._x4 = i, this._y4 = e, this._context.moveTo((this._x0 + 4 * this._x1 + i) / 6, (this._y0 + 4 * this._y1 + e) / 6);
        break;
      default:
        K(this, i, e);
        break;
    }
    this._x0 = this._x1, this._x1 = i, this._y0 = this._y1, this._y1 = e;
  }
};
function Ue(i) {
  return new Vi(i);
}
function Xi(i) {
  this._context = i;
}
Xi.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._x0 = this._x1 = this._y0 = this._y1 = NaN, this._point = 0;
  },
  lineEnd: function() {
    (this._line || this._line !== 0 && this._point === 3) && this._context.closePath(), this._line = 1 - this._line;
  },
  point: function(i, e) {
    switch (i = +i, e = +e, this._point) {
      case 0:
        this._point = 1;
        break;
      case 1:
        this._point = 2;
        break;
      case 2:
        this._point = 3;
        var n = (this._x0 + 4 * this._x1 + i) / 6, t = (this._y0 + 4 * this._y1 + e) / 6;
        this._line ? this._context.lineTo(n, t) : this._context.moveTo(n, t);
        break;
      case 3:
        this._point = 4;
      default:
        K(this, i, e);
        break;
    }
    this._x0 = this._x1, this._x1 = i, this._y0 = this._y1, this._y1 = e;
  }
};
function He(i) {
  return new Xi(i);
}
function Ji(i, e) {
  this._basis = new ri(i), this._beta = e;
}
Ji.prototype = {
  lineStart: function() {
    this._x = [], this._y = [], this._basis.lineStart();
  },
  lineEnd: function() {
    var i = this._x, e = this._y, n = i.length - 1;
    if (n > 0)
      for (var t = i[0], r = e[0], s = i[n] - t, o = e[n] - r, l = -1, a; ++l <= n; )
        a = l / n, this._basis.point(
          this._beta * i[l] + (1 - this._beta) * (t + a * s),
          this._beta * e[l] + (1 - this._beta) * (r + a * o)
        );
    this._x = this._y = null, this._basis.lineEnd();
  },
  point: function(i, e) {
    this._x.push(+i), this._y.push(+e);
  }
};
const Ge = function i(e) {
  function n(t) {
    return e === 1 ? new ri(t) : new Ji(t, e);
  }
  return n.beta = function(t) {
    return i(+t);
  }, n;
}(0.85);
function V(i, e, n) {
  i._context.bezierCurveTo(
    i._x1 + i._k * (i._x2 - i._x0),
    i._y1 + i._k * (i._y2 - i._y0),
    i._x2 + i._k * (i._x1 - e),
    i._y2 + i._k * (i._y1 - n),
    i._x2,
    i._y2
  );
}
function yi(i, e) {
  this._context = i, this._k = (1 - e) / 6;
}
yi.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._x0 = this._x1 = this._x2 = this._y0 = this._y1 = this._y2 = NaN, this._point = 0;
  },
  lineEnd: function() {
    switch (this._point) {
      case 2:
        this._context.lineTo(this._x2, this._y2);
        break;
      case 3:
        V(this, this._x1, this._y1);
        break;
    }
    (this._line || this._line !== 0 && this._point === 1) && this._context.closePath(), this._line = 1 - this._line;
  },
  point: function(i, e) {
    switch (i = +i, e = +e, this._point) {
      case 0:
        this._point = 1, this._line ? this._context.lineTo(i, e) : this._context.moveTo(i, e);
        break;
      case 1:
        this._point = 2, this._x1 = i, this._y1 = e;
        break;
      case 2:
        this._point = 3;
      default:
        V(this, i, e);
        break;
    }
    this._x0 = this._x1, this._x1 = this._x2, this._x2 = i, this._y0 = this._y1, this._y1 = this._y2, this._y2 = e;
  }
};
const qe = function i(e) {
  function n(t) {
    return new yi(t, e);
  }
  return n.tension = function(t) {
    return i(+t);
  }, n;
}(0);
function vi(i, e) {
  this._context = i, this._k = (1 - e) / 6;
}
vi.prototype = {
  areaStart: E,
  areaEnd: E,
  lineStart: function() {
    this._x0 = this._x1 = this._x2 = this._x3 = this._x4 = this._x5 = this._y0 = this._y1 = this._y2 = this._y3 = this._y4 = this._y5 = NaN, this._point = 0;
  },
  lineEnd: function() {
    switch (this._point) {
      case 1: {
        this._context.moveTo(this._x3, this._y3), this._context.closePath();
        break;
      }
      case 2: {
        this._context.lineTo(this._x3, this._y3), this._context.closePath();
        break;
      }
      case 3: {
        this.point(this._x3, this._y3), this.point(this._x4, this._y4), this.point(this._x5, this._y5);
        break;
      }
    }
  },
  point: function(i, e) {
    switch (i = +i, e = +e, this._point) {
      case 0:
        this._point = 1, this._x3 = i, this._y3 = e;
        break;
      case 1:
        this._point = 2, this._context.moveTo(this._x4 = i, this._y4 = e);
        break;
      case 2:
        this._point = 3, this._x5 = i, this._y5 = e;
        break;
      default:
        V(this, i, e);
        break;
    }
    this._x0 = this._x1, this._x1 = this._x2, this._x2 = i, this._y0 = this._y1, this._y1 = this._y2, this._y2 = e;
  }
};
const Ke = function i(e) {
  function n(t) {
    return new vi(t, e);
  }
  return n.tension = function(t) {
    return i(+t);
  }, n;
}(0);
function bi(i, e) {
  this._context = i, this._k = (1 - e) / 6;
}
bi.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._x0 = this._x1 = this._x2 = this._y0 = this._y1 = this._y2 = NaN, this._point = 0;
  },
  lineEnd: function() {
    (this._line || this._line !== 0 && this._point === 3) && this._context.closePath(), this._line = 1 - this._line;
  },
  point: function(i, e) {
    switch (i = +i, e = +e, this._point) {
      case 0:
        this._point = 1;
        break;
      case 1:
        this._point = 2;
        break;
      case 2:
        this._point = 3, this._line ? this._context.lineTo(this._x2, this._y2) : this._context.moveTo(this._x2, this._y2);
        break;
      case 3:
        this._point = 4;
      default:
        V(this, i, e);
        break;
    }
    this._x0 = this._x1, this._x1 = this._x2, this._x2 = i, this._y0 = this._y1, this._y1 = this._y2, this._y2 = e;
  }
};
const Ve = function i(e) {
  function n(t) {
    return new bi(t, e);
  }
  return n.tension = function(t) {
    return i(+t);
  }, n;
}(0);
function wi(i, e, n) {
  var t = i._x1, r = i._y1, s = i._x2, o = i._y2;
  if (i._l01_a > Ni) {
    var l = 2 * i._l01_2a + 3 * i._l01_a * i._l12_a + i._l12_2a, a = 3 * i._l01_a * (i._l01_a + i._l12_a);
    t = (t * l - i._x0 * i._l12_2a + i._x2 * i._l01_2a) / a, r = (r * l - i._y0 * i._l12_2a + i._y2 * i._l01_2a) / a;
  }
  if (i._l23_a > Ni) {
    var h = 2 * i._l23_2a + 3 * i._l23_a * i._l12_a + i._l12_2a, f = 3 * i._l23_a * (i._l23_a + i._l12_a);
    s = (s * h + i._x1 * i._l23_2a - e * i._l12_2a) / f, o = (o * h + i._y1 * i._l23_2a - n * i._l12_2a) / f;
  }
  i._context.bezierCurveTo(t, r, s, o, i._x2, i._y2);
}
function Zi(i, e) {
  this._context = i, this._alpha = e;
}
Zi.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._x0 = this._x1 = this._x2 = this._y0 = this._y1 = this._y2 = NaN, this._l01_a = this._l12_a = this._l23_a = this._l01_2a = this._l12_2a = this._l23_2a = this._point = 0;
  },
  lineEnd: function() {
    switch (this._point) {
      case 2:
        this._context.lineTo(this._x2, this._y2);
        break;
      case 3:
        this.point(this._x2, this._y2);
        break;
    }
    (this._line || this._line !== 0 && this._point === 1) && this._context.closePath(), this._line = 1 - this._line;
  },
  point: function(i, e) {
    if (i = +i, e = +e, this._point) {
      var n = this._x2 - i, t = this._y2 - e;
      this._l23_a = Math.sqrt(this._l23_2a = Math.pow(n * n + t * t, this._alpha));
    }
    switch (this._point) {
      case 0:
        this._point = 1, this._line ? this._context.lineTo(i, e) : this._context.moveTo(i, e);
        break;
      case 1:
        this._point = 2;
        break;
      case 2:
        this._point = 3;
      default:
        wi(this, i, e);
        break;
    }
    this._l01_a = this._l12_a, this._l12_a = this._l23_a, this._l01_2a = this._l12_2a, this._l12_2a = this._l23_2a, this._x0 = this._x1, this._x1 = this._x2, this._x2 = i, this._y0 = this._y1, this._y1 = this._y2, this._y2 = e;
  }
};
const Xe = function i(e) {
  function n(t) {
    return e ? new Zi(t, e) : new yi(t, 0);
  }
  return n.alpha = function(t) {
    return i(+t);
  }, n;
}(0.5);
function Qi(i, e) {
  this._context = i, this._alpha = e;
}
Qi.prototype = {
  areaStart: E,
  areaEnd: E,
  lineStart: function() {
    this._x0 = this._x1 = this._x2 = this._x3 = this._x4 = this._x5 = this._y0 = this._y1 = this._y2 = this._y3 = this._y4 = this._y5 = NaN, this._l01_a = this._l12_a = this._l23_a = this._l01_2a = this._l12_2a = this._l23_2a = this._point = 0;
  },
  lineEnd: function() {
    switch (this._point) {
      case 1: {
        this._context.moveTo(this._x3, this._y3), this._context.closePath();
        break;
      }
      case 2: {
        this._context.lineTo(this._x3, this._y3), this._context.closePath();
        break;
      }
      case 3: {
        this.point(this._x3, this._y3), this.point(this._x4, this._y4), this.point(this._x5, this._y5);
        break;
      }
    }
  },
  point: function(i, e) {
    if (i = +i, e = +e, this._point) {
      var n = this._x2 - i, t = this._y2 - e;
      this._l23_a = Math.sqrt(this._l23_2a = Math.pow(n * n + t * t, this._alpha));
    }
    switch (this._point) {
      case 0:
        this._point = 1, this._x3 = i, this._y3 = e;
        break;
      case 1:
        this._point = 2, this._context.moveTo(this._x4 = i, this._y4 = e);
        break;
      case 2:
        this._point = 3, this._x5 = i, this._y5 = e;
        break;
      default:
        wi(this, i, e);
        break;
    }
    this._l01_a = this._l12_a, this._l12_a = this._l23_a, this._l01_2a = this._l12_2a, this._l12_2a = this._l23_2a, this._x0 = this._x1, this._x1 = this._x2, this._x2 = i, this._y0 = this._y1, this._y1 = this._y2, this._y2 = e;
  }
};
const Je = function i(e) {
  function n(t) {
    return e ? new Qi(t, e) : new vi(t, 0);
  }
  return n.alpha = function(t) {
    return i(+t);
  }, n;
}(0.5);
function ie(i, e) {
  this._context = i, this._alpha = e;
}
ie.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._x0 = this._x1 = this._x2 = this._y0 = this._y1 = this._y2 = NaN, this._l01_a = this._l12_a = this._l23_a = this._l01_2a = this._l12_2a = this._l23_2a = this._point = 0;
  },
  lineEnd: function() {
    (this._line || this._line !== 0 && this._point === 3) && this._context.closePath(), this._line = 1 - this._line;
  },
  point: function(i, e) {
    if (i = +i, e = +e, this._point) {
      var n = this._x2 - i, t = this._y2 - e;
      this._l23_a = Math.sqrt(this._l23_2a = Math.pow(n * n + t * t, this._alpha));
    }
    switch (this._point) {
      case 0:
        this._point = 1;
        break;
      case 1:
        this._point = 2;
        break;
      case 2:
        this._point = 3, this._line ? this._context.lineTo(this._x2, this._y2) : this._context.moveTo(this._x2, this._y2);
        break;
      case 3:
        this._point = 4;
      default:
        wi(this, i, e);
        break;
    }
    this._l01_a = this._l12_a, this._l12_a = this._l23_a, this._l01_2a = this._l12_2a, this._l12_2a = this._l23_2a, this._x0 = this._x1, this._x1 = this._x2, this._x2 = i, this._y0 = this._y1, this._y1 = this._y2, this._y2 = e;
  }
};
const Ze = function i(e) {
  function n(t) {
    return e ? new ie(t, e) : new bi(t, 0);
  }
  return n.alpha = function(t) {
    return i(+t);
  }, n;
}(0.5);
function ee(i) {
  this._context = i;
}
ee.prototype = {
  areaStart: E,
  areaEnd: E,
  lineStart: function() {
    this._point = 0;
  },
  lineEnd: function() {
    this._point && this._context.closePath();
  },
  point: function(i, e) {
    i = +i, e = +e, this._point ? this._context.lineTo(i, e) : (this._point = 1, this._context.moveTo(i, e));
  }
};
function Qe(i) {
  return new ee(i);
}
function Fi(i) {
  return i < 0 ? -1 : 1;
}
function $i(i, e, n) {
  var t = i._x1 - i._x0, r = e - i._x1, s = (i._y1 - i._y0) / (t || r < 0 && -0), o = (n - i._y1) / (r || t < 0 && -0), l = (s * r + o * t) / (t + r);
  return (Fi(s) + Fi(o)) * Math.min(Math.abs(s), Math.abs(o), 0.5 * Math.abs(l)) || 0;
}
function Mi(i, e) {
  var n = i._x1 - i._x0;
  return n ? (3 * (i._y1 - i._y0) / n - e) / 2 : e;
}
function ci(i, e, n) {
  var t = i._x0, r = i._y0, s = i._x1, o = i._y1, l = (s - t) / 3;
  i._context.bezierCurveTo(t + l, r + l * e, s - l, o - l * n, s, o);
}
function X(i) {
  this._context = i;
}
X.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._x0 = this._x1 = this._y0 = this._y1 = this._t0 = NaN, this._point = 0;
  },
  lineEnd: function() {
    switch (this._point) {
      case 2:
        this._context.lineTo(this._x1, this._y1);
        break;
      case 3:
        ci(this, this._t0, Mi(this, this._t0));
        break;
    }
    (this._line || this._line !== 0 && this._point === 1) && this._context.closePath(), this._line = 1 - this._line;
  },
  point: function(i, e) {
    var n = NaN;
    if (i = +i, e = +e, !(i === this._x1 && e === this._y1)) {
      switch (this._point) {
        case 0:
          this._point = 1, this._line ? this._context.lineTo(i, e) : this._context.moveTo(i, e);
          break;
        case 1:
          this._point = 2;
          break;
        case 2:
          this._point = 3, ci(this, Mi(this, n = $i(this, i, e)), n);
          break;
        default:
          ci(this, this._t0, n = $i(this, i, e));
          break;
      }
      this._x0 = this._x1, this._x1 = i, this._y0 = this._y1, this._y1 = e, this._t0 = n;
    }
  }
};
function ne(i) {
  this._context = new te(i);
}
(ne.prototype = Object.create(X.prototype)).point = function(i, e) {
  X.prototype.point.call(this, e, i);
};
function te(i) {
  this._context = i;
}
te.prototype = {
  moveTo: function(i, e) {
    this._context.moveTo(e, i);
  },
  closePath: function() {
    this._context.closePath();
  },
  lineTo: function(i, e) {
    this._context.lineTo(e, i);
  },
  bezierCurveTo: function(i, e, n, t, r, s) {
    this._context.bezierCurveTo(e, i, t, n, s, r);
  }
};
function en(i) {
  return new X(i);
}
function nn(i) {
  return new ne(i);
}
function re(i) {
  this._context = i;
}
re.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._x = [], this._y = [];
  },
  lineEnd: function() {
    var i = this._x, e = this._y, n = i.length;
    if (n)
      if (this._line ? this._context.lineTo(i[0], e[0]) : this._context.moveTo(i[0], e[0]), n === 2)
        this._context.lineTo(i[1], e[1]);
      else
        for (var t = Ii(i), r = Ii(e), s = 0, o = 1; o < n; ++s, ++o)
          this._context.bezierCurveTo(t[0][s], r[0][s], t[1][s], r[1][s], i[o], e[o]);
    (this._line || this._line !== 0 && n === 1) && this._context.closePath(), this._line = 1 - this._line, this._x = this._y = null;
  },
  point: function(i, e) {
    this._x.push(+i), this._y.push(+e);
  }
};
function Ii(i) {
  var e, n = i.length - 1, t, r = new Array(n), s = new Array(n), o = new Array(n);
  for (r[0] = 0, s[0] = 2, o[0] = i[0] + 2 * i[1], e = 1; e < n - 1; ++e)
    r[e] = 1, s[e] = 4, o[e] = 4 * i[e] + 2 * i[e + 1];
  for (r[n - 1] = 2, s[n - 1] = 7, o[n - 1] = 8 * i[n - 1] + i[n], e = 1; e < n; ++e)
    t = r[e] / s[e - 1], s[e] -= t, o[e] -= t * o[e - 1];
  for (r[n - 1] = o[n - 1] / s[n - 1], e = n - 2; e >= 0; --e)
    r[e] = (o[e] - r[e + 1]) / s[e];
  for (s[n - 1] = (i[n] + r[n - 1]) / 2, e = 0; e < n - 1; ++e)
    s[e] = 2 * i[e + 1] - r[e + 1];
  return [r, s];
}
function tn(i) {
  return new re(i);
}
function oi(i, e) {
  this._context = i, this._t = e;
}
oi.prototype = {
  areaStart: function() {
    this._line = 0;
  },
  areaEnd: function() {
    this._line = NaN;
  },
  lineStart: function() {
    this._x = this._y = NaN, this._point = 0;
  },
  lineEnd: function() {
    0 < this._t && this._t < 1 && this._point === 2 && this._context.lineTo(this._x, this._y), (this._line || this._line !== 0 && this._point === 1) && this._context.closePath(), this._line >= 0 && (this._t = 1 - this._t, this._line = 1 - this._line);
  },
  point: function(i, e) {
    switch (i = +i, e = +e, this._point) {
      case 0:
        this._point = 1, this._line ? this._context.lineTo(i, e) : this._context.moveTo(i, e);
        break;
      case 1:
        this._point = 2;
      default: {
        if (this._t <= 0)
          this._context.lineTo(this._x, e), this._context.lineTo(i, e);
        else {
          var n = this._x * (1 - this._t) + i * this._t;
          this._context.lineTo(n, this._y), this._context.lineTo(n, e);
        }
        break;
      }
    }
    this._x = i, this._y = e;
  }
};
function rn(i) {
  return new oi(i, 0.5);
}
function on(i) {
  return new oi(i, 0);
}
function sn(i) {
  return new oi(i, 1);
}
/*! js-yaml 4.1.0 https://github.com/nodeca/js-yaml @license MIT */
function oe(i) {
  return typeof i > "u" || i === null;
}
function ln(i) {
  return typeof i == "object" && i !== null;
}
function an(i) {
  return Array.isArray(i) ? i : oe(i) ? [] : [i];
}
function cn(i, e) {
  var n, t, r, s;
  if (e)
    for (s = Object.keys(e), n = 0, t = s.length; n < t; n += 1)
      r = s[n], i[r] = e[r];
  return i;
}
function hn(i, e) {
  var n = "", t;
  for (t = 0; t < e; t += 1)
    n += i;
  return n;
}
function un(i) {
  return i === 0 && Number.NEGATIVE_INFINITY === 1 / i;
}
var fn = oe, _n = ln, pn = an, dn = hn, xn = un, gn = cn, w = {
  isNothing: fn,
  isObject: _n,
  toArray: pn,
  repeat: dn,
  isNegativeZero: xn,
  extend: gn
};
function se(i, e) {
  var n = "", t = i.reason || "(unknown reason)";
  return i.mark ? (i.mark.name && (n += 'in "' + i.mark.name + '" '), n += "(" + (i.mark.line + 1) + ":" + (i.mark.column + 1) + ")", !e && i.mark.snippet && (n += `

` + i.mark.snippet), t + " " + n) : t;
}
function B(i, e) {
  Error.call(this), this.name = "YAMLException", this.reason = i, this.mark = e, this.message = se(this, !1), Error.captureStackTrace ? Error.captureStackTrace(this, this.constructor) : this.stack = new Error().stack || "";
}
B.prototype = Object.create(Error.prototype);
B.prototype.constructor = B;
B.prototype.toString = function(e) {
  return this.name + ": " + se(this, e);
};
var T = B;
function hi(i, e, n, t, r) {
  var s = "", o = "", l = Math.floor(r / 2) - 1;
  return t - e > l && (s = " ... ", e = t - l + s.length), n - t > l && (o = " ...", n = t + l - o.length), {
    str: s + i.slice(e, n).replace(/\t/g, "→") + o,
    pos: t - e + s.length
    // relative position
  };
}
function ui(i, e) {
  return w.repeat(" ", e - i.length) + i;
}
function mn(i, e) {
  if (e = Object.create(e || null), !i.buffer)
    return null;
  e.maxLength || (e.maxLength = 79), typeof e.indent != "number" && (e.indent = 1), typeof e.linesBefore != "number" && (e.linesBefore = 3), typeof e.linesAfter != "number" && (e.linesAfter = 2);
  for (var n = /\r?\n|\r|\0/g, t = [0], r = [], s, o = -1; s = n.exec(i.buffer); )
    r.push(s.index), t.push(s.index + s[0].length), i.position <= s.index && o < 0 && (o = t.length - 2);
  o < 0 && (o = t.length - 1);
  var l = "", a, h, f = Math.min(i.line + e.linesAfter, r.length).toString().length, c = e.maxLength - (e.indent + f + 3);
  for (a = 1; a <= e.linesBefore && !(o - a < 0); a++)
    h = hi(
      i.buffer,
      t[o - a],
      r[o - a],
      i.position - (t[o] - t[o - a]),
      c
    ), l = w.repeat(" ", e.indent) + ui((i.line - a + 1).toString(), f) + " | " + h.str + `
` + l;
  for (h = hi(i.buffer, t[o], r[o], i.position, c), l += w.repeat(" ", e.indent) + ui((i.line + 1).toString(), f) + " | " + h.str + `
`, l += w.repeat("-", e.indent + f + 3 + h.pos) + `^
`, a = 1; a <= e.linesAfter && !(o + a >= r.length); a++)
    h = hi(
      i.buffer,
      t[o + a],
      r[o + a],
      i.position - (t[o] - t[o + a]),
      c
    ), l += w.repeat(" ", e.indent) + ui((i.line + a + 1).toString(), f) + " | " + h.str + `
`;
  return l.replace(/\n$/, "");
}
var yn = mn, vn = [
  "kind",
  "multi",
  "resolve",
  "construct",
  "instanceOf",
  "predicate",
  "represent",
  "representName",
  "defaultStyle",
  "styleAliases"
], bn = [
  "scalar",
  "sequence",
  "mapping"
];
function wn(i) {
  var e = {};
  return i !== null && Object.keys(i).forEach(function(n) {
    i[n].forEach(function(t) {
      e[String(t)] = n;
    });
  }), e;
}
function Cn(i, e) {
  if (e = e || {}, Object.keys(e).forEach(function(n) {
    if (vn.indexOf(n) === -1)
      throw new T('Unknown option "' + n + '" is met in definition of "' + i + '" YAML type.');
  }), this.options = e, this.tag = i, this.kind = e.kind || null, this.resolve = e.resolve || function() {
    return !0;
  }, this.construct = e.construct || function(n) {
    return n;
  }, this.instanceOf = e.instanceOf || null, this.predicate = e.predicate || null, this.represent = e.represent || null, this.representName = e.representName || null, this.defaultStyle = e.defaultStyle || null, this.multi = e.multi || !1, this.styleAliases = wn(e.styleAliases || null), bn.indexOf(this.kind) === -1)
    throw new T('Unknown kind "' + this.kind + '" is specified for "' + i + '" YAML type.');
}
var b = Cn;
function Pi(i, e) {
  var n = [];
  return i[e].forEach(function(t) {
    var r = n.length;
    n.forEach(function(s, o) {
      s.tag === t.tag && s.kind === t.kind && s.multi === t.multi && (r = o);
    }), n[r] = t;
  }), n;
}
function An() {
  var i = {
    scalar: {},
    sequence: {},
    mapping: {},
    fallback: {},
    multi: {
      scalar: [],
      sequence: [],
      mapping: [],
      fallback: []
    }
  }, e, n;
  function t(r) {
    r.multi ? (i.multi[r.kind].push(r), i.multi.fallback.push(r)) : i[r.kind][r.tag] = i.fallback[r.tag] = r;
  }
  for (e = 0, n = arguments.length; e < n; e += 1)
    arguments[e].forEach(t);
  return i;
}
function di(i) {
  return this.extend(i);
}
di.prototype.extend = function(e) {
  var n = [], t = [];
  if (e instanceof b)
    t.push(e);
  else if (Array.isArray(e))
    t = t.concat(e);
  else if (e && (Array.isArray(e.implicit) || Array.isArray(e.explicit)))
    e.implicit && (n = n.concat(e.implicit)), e.explicit && (t = t.concat(e.explicit));
  else
    throw new T("Schema.extend argument should be a Type, [ Type ], or a schema definition ({ implicit: [...], explicit: [...] })");
  n.forEach(function(s) {
    if (!(s instanceof b))
      throw new T("Specified list of YAML types (or a single Type object) contains a non-Type object.");
    if (s.loadKind && s.loadKind !== "scalar")
      throw new T("There is a non-scalar type in the implicit list of a schema. Implicit resolving of such types is not supported.");
    if (s.multi)
      throw new T("There is a multi type in the implicit list of a schema. Multi tags can only be listed as explicit.");
  }), t.forEach(function(s) {
    if (!(s instanceof b))
      throw new T("Specified list of YAML types (or a single Type object) contains a non-Type object.");
  });
  var r = Object.create(di.prototype);
  return r.implicit = (this.implicit || []).concat(n), r.explicit = (this.explicit || []).concat(t), r.compiledImplicit = Pi(r, "implicit"), r.compiledExplicit = Pi(r, "explicit"), r.compiledTypeMap = An(r.compiledImplicit, r.compiledExplicit), r;
};
var Sn = di, Tn = new b("tag:yaml.org,2002:str", {
  kind: "scalar",
  construct: function(i) {
    return i !== null ? i : "";
  }
}), kn = new b("tag:yaml.org,2002:seq", {
  kind: "sequence",
  construct: function(i) {
    return i !== null ? i : [];
  }
}), En = new b("tag:yaml.org,2002:map", {
  kind: "mapping",
  construct: function(i) {
    return i !== null ? i : {};
  }
}), le = new Sn({
  explicit: [
    Tn,
    kn,
    En
  ]
});
function Nn(i) {
  if (i === null)
    return !0;
  var e = i.length;
  return e === 1 && i === "~" || e === 4 && (i === "null" || i === "Null" || i === "NULL");
}
function On() {
  return null;
}
function Fn(i) {
  return i === null;
}
var $n = new b("tag:yaml.org,2002:null", {
  kind: "scalar",
  resolve: Nn,
  construct: On,
  predicate: Fn,
  represent: {
    canonical: function() {
      return "~";
    },
    lowercase: function() {
      return "null";
    },
    uppercase: function() {
      return "NULL";
    },
    camelcase: function() {
      return "Null";
    },
    empty: function() {
      return "";
    }
  },
  defaultStyle: "lowercase"
});
function Mn(i) {
  if (i === null)
    return !1;
  var e = i.length;
  return e === 4 && (i === "true" || i === "True" || i === "TRUE") || e === 5 && (i === "false" || i === "False" || i === "FALSE");
}
function In(i) {
  return i === "true" || i === "True" || i === "TRUE";
}
function Pn(i) {
  return Object.prototype.toString.call(i) === "[object Boolean]";
}
var jn = new b("tag:yaml.org,2002:bool", {
  kind: "scalar",
  resolve: Mn,
  construct: In,
  predicate: Pn,
  represent: {
    lowercase: function(i) {
      return i ? "true" : "false";
    },
    uppercase: function(i) {
      return i ? "TRUE" : "FALSE";
    },
    camelcase: function(i) {
      return i ? "True" : "False";
    }
  },
  defaultStyle: "lowercase"
});
function Ln(i) {
  return 48 <= i && i <= 57 || 65 <= i && i <= 70 || 97 <= i && i <= 102;
}
function Dn(i) {
  return 48 <= i && i <= 55;
}
function Rn(i) {
  return 48 <= i && i <= 57;
}
function zn(i) {
  if (i === null)
    return !1;
  var e = i.length, n = 0, t = !1, r;
  if (!e)
    return !1;
  if (r = i[n], (r === "-" || r === "+") && (r = i[++n]), r === "0") {
    if (n + 1 === e)
      return !0;
    if (r = i[++n], r === "b") {
      for (n++; n < e; n++)
        if (r = i[n], r !== "_") {
          if (r !== "0" && r !== "1")
            return !1;
          t = !0;
        }
      return t && r !== "_";
    }
    if (r === "x") {
      for (n++; n < e; n++)
        if (r = i[n], r !== "_") {
          if (!Ln(i.charCodeAt(n)))
            return !1;
          t = !0;
        }
      return t && r !== "_";
    }
    if (r === "o") {
      for (n++; n < e; n++)
        if (r = i[n], r !== "_") {
          if (!Dn(i.charCodeAt(n)))
            return !1;
          t = !0;
        }
      return t && r !== "_";
    }
  }
  if (r === "_")
    return !1;
  for (; n < e; n++)
    if (r = i[n], r !== "_") {
      if (!Rn(i.charCodeAt(n)))
        return !1;
      t = !0;
    }
  return !(!t || r === "_");
}
function Bn(i) {
  var e = i, n = 1, t;
  if (e.indexOf("_") !== -1 && (e = e.replace(/_/g, "")), t = e[0], (t === "-" || t === "+") && (t === "-" && (n = -1), e = e.slice(1), t = e[0]), e === "0")
    return 0;
  if (t === "0") {
    if (e[1] === "b")
      return n * parseInt(e.slice(2), 2);
    if (e[1] === "x")
      return n * parseInt(e.slice(2), 16);
    if (e[1] === "o")
      return n * parseInt(e.slice(2), 8);
  }
  return n * parseInt(e, 10);
}
function Yn(i) {
  return Object.prototype.toString.call(i) === "[object Number]" && i % 1 === 0 && !w.isNegativeZero(i);
}
var Wn = new b("tag:yaml.org,2002:int", {
  kind: "scalar",
  resolve: zn,
  construct: Bn,
  predicate: Yn,
  represent: {
    binary: function(i) {
      return i >= 0 ? "0b" + i.toString(2) : "-0b" + i.toString(2).slice(1);
    },
    octal: function(i) {
      return i >= 0 ? "0o" + i.toString(8) : "-0o" + i.toString(8).slice(1);
    },
    decimal: function(i) {
      return i.toString(10);
    },
    /* eslint-disable max-len */
    hexadecimal: function(i) {
      return i >= 0 ? "0x" + i.toString(16).toUpperCase() : "-0x" + i.toString(16).toUpperCase().slice(1);
    }
  },
  defaultStyle: "decimal",
  styleAliases: {
    binary: [2, "bin"],
    octal: [8, "oct"],
    decimal: [10, "dec"],
    hexadecimal: [16, "hex"]
  }
}), Un = new RegExp(
  // 2.5e4, 2.5 and integers
  "^(?:[-+]?(?:[0-9][0-9_]*)(?:\\.[0-9_]*)?(?:[eE][-+]?[0-9]+)?|\\.[0-9_]+(?:[eE][-+]?[0-9]+)?|[-+]?\\.(?:inf|Inf|INF)|\\.(?:nan|NaN|NAN))$"
);
function Hn(i) {
  return !(i === null || !Un.test(i) || // Quick hack to not allow integers end with `_`
  // Probably should update regexp & check speed
  i[i.length - 1] === "_");
}
function Gn(i) {
  var e, n;
  return e = i.replace(/_/g, "").toLowerCase(), n = e[0] === "-" ? -1 : 1, "+-".indexOf(e[0]) >= 0 && (e = e.slice(1)), e === ".inf" ? n === 1 ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY : e === ".nan" ? NaN : n * parseFloat(e, 10);
}
var qn = /^[-+]?[0-9]+e/;
function Kn(i, e) {
  var n;
  if (isNaN(i))
    switch (e) {
      case "lowercase":
        return ".nan";
      case "uppercase":
        return ".NAN";
      case "camelcase":
        return ".NaN";
    }
  else if (Number.POSITIVE_INFINITY === i)
    switch (e) {
      case "lowercase":
        return ".inf";
      case "uppercase":
        return ".INF";
      case "camelcase":
        return ".Inf";
    }
  else if (Number.NEGATIVE_INFINITY === i)
    switch (e) {
      case "lowercase":
        return "-.inf";
      case "uppercase":
        return "-.INF";
      case "camelcase":
        return "-.Inf";
    }
  else if (w.isNegativeZero(i))
    return "-0.0";
  return n = i.toString(10), qn.test(n) ? n.replace("e", ".e") : n;
}
function Vn(i) {
  return Object.prototype.toString.call(i) === "[object Number]" && (i % 1 !== 0 || w.isNegativeZero(i));
}
var Xn = new b("tag:yaml.org,2002:float", {
  kind: "scalar",
  resolve: Hn,
  construct: Gn,
  predicate: Vn,
  represent: Kn,
  defaultStyle: "lowercase"
}), Jn = le.extend({
  implicit: [
    $n,
    jn,
    Wn,
    Xn
  ]
}), Zn = Jn, ae = new RegExp(
  "^([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])$"
), ce = new RegExp(
  "^([0-9][0-9][0-9][0-9])-([0-9][0-9]?)-([0-9][0-9]?)(?:[Tt]|[ \\t]+)([0-9][0-9]?):([0-9][0-9]):([0-9][0-9])(?:\\.([0-9]*))?(?:[ \\t]*(Z|([-+])([0-9][0-9]?)(?::([0-9][0-9]))?))?$"
);
function Qn(i) {
  return i === null ? !1 : ae.exec(i) !== null || ce.exec(i) !== null;
}
function it(i) {
  var e, n, t, r, s, o, l, a = 0, h = null, f, c, p;
  if (e = ae.exec(i), e === null && (e = ce.exec(i)), e === null)
    throw new Error("Date resolve error");
  if (n = +e[1], t = +e[2] - 1, r = +e[3], !e[4])
    return new Date(Date.UTC(n, t, r));
  if (s = +e[4], o = +e[5], l = +e[6], e[7]) {
    for (a = e[7].slice(0, 3); a.length < 3; )
      a += "0";
    a = +a;
  }
  return e[9] && (f = +e[10], c = +(e[11] || 0), h = (f * 60 + c) * 6e4, e[9] === "-" && (h = -h)), p = new Date(Date.UTC(n, t, r, s, o, l, a)), h && p.setTime(p.getTime() - h), p;
}
function et(i) {
  return i.toISOString();
}
var nt = new b("tag:yaml.org,2002:timestamp", {
  kind: "scalar",
  resolve: Qn,
  construct: it,
  instanceOf: Date,
  represent: et
});
function tt(i) {
  return i === "<<" || i === null;
}
var rt = new b("tag:yaml.org,2002:merge", {
  kind: "scalar",
  resolve: tt
}), Ci = `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=
\r`;
function ot(i) {
  if (i === null)
    return !1;
  var e, n, t = 0, r = i.length, s = Ci;
  for (n = 0; n < r; n++)
    if (e = s.indexOf(i.charAt(n)), !(e > 64)) {
      if (e < 0)
        return !1;
      t += 6;
    }
  return t % 8 === 0;
}
function st(i) {
  var e, n, t = i.replace(/[\r\n=]/g, ""), r = t.length, s = Ci, o = 0, l = [];
  for (e = 0; e < r; e++)
    e % 4 === 0 && e && (l.push(o >> 16 & 255), l.push(o >> 8 & 255), l.push(o & 255)), o = o << 6 | s.indexOf(t.charAt(e));
  return n = r % 4 * 6, n === 0 ? (l.push(o >> 16 & 255), l.push(o >> 8 & 255), l.push(o & 255)) : n === 18 ? (l.push(o >> 10 & 255), l.push(o >> 2 & 255)) : n === 12 && l.push(o >> 4 & 255), new Uint8Array(l);
}
function lt(i) {
  var e = "", n = 0, t, r, s = i.length, o = Ci;
  for (t = 0; t < s; t++)
    t % 3 === 0 && t && (e += o[n >> 18 & 63], e += o[n >> 12 & 63], e += o[n >> 6 & 63], e += o[n & 63]), n = (n << 8) + i[t];
  return r = s % 3, r === 0 ? (e += o[n >> 18 & 63], e += o[n >> 12 & 63], e += o[n >> 6 & 63], e += o[n & 63]) : r === 2 ? (e += o[n >> 10 & 63], e += o[n >> 4 & 63], e += o[n << 2 & 63], e += o[64]) : r === 1 && (e += o[n >> 2 & 63], e += o[n << 4 & 63], e += o[64], e += o[64]), e;
}
function at(i) {
  return Object.prototype.toString.call(i) === "[object Uint8Array]";
}
var ct = new b("tag:yaml.org,2002:binary", {
  kind: "scalar",
  resolve: ot,
  construct: st,
  predicate: at,
  represent: lt
}), ht = Object.prototype.hasOwnProperty, ut = Object.prototype.toString;
function ft(i) {
  if (i === null)
    return !0;
  var e = [], n, t, r, s, o, l = i;
  for (n = 0, t = l.length; n < t; n += 1) {
    if (r = l[n], o = !1, ut.call(r) !== "[object Object]")
      return !1;
    for (s in r)
      if (ht.call(r, s))
        if (!o)
          o = !0;
        else
          return !1;
    if (!o)
      return !1;
    if (e.indexOf(s) === -1)
      e.push(s);
    else
      return !1;
  }
  return !0;
}
function _t(i) {
  return i !== null ? i : [];
}
var pt = new b("tag:yaml.org,2002:omap", {
  kind: "sequence",
  resolve: ft,
  construct: _t
}), dt = Object.prototype.toString;
function xt(i) {
  if (i === null)
    return !0;
  var e, n, t, r, s, o = i;
  for (s = new Array(o.length), e = 0, n = o.length; e < n; e += 1) {
    if (t = o[e], dt.call(t) !== "[object Object]" || (r = Object.keys(t), r.length !== 1))
      return !1;
    s[e] = [r[0], t[r[0]]];
  }
  return !0;
}
function gt(i) {
  if (i === null)
    return [];
  var e, n, t, r, s, o = i;
  for (s = new Array(o.length), e = 0, n = o.length; e < n; e += 1)
    t = o[e], r = Object.keys(t), s[e] = [r[0], t[r[0]]];
  return s;
}
var mt = new b("tag:yaml.org,2002:pairs", {
  kind: "sequence",
  resolve: xt,
  construct: gt
}), yt = Object.prototype.hasOwnProperty;
function vt(i) {
  if (i === null)
    return !0;
  var e, n = i;
  for (e in n)
    if (yt.call(n, e) && n[e] !== null)
      return !1;
  return !0;
}
function bt(i) {
  return i !== null ? i : {};
}
var wt = new b("tag:yaml.org,2002:set", {
  kind: "mapping",
  resolve: vt,
  construct: bt
}), Ct = Zn.extend({
  implicit: [
    nt,
    rt
  ],
  explicit: [
    ct,
    pt,
    mt,
    wt
  ]
}), N = Object.prototype.hasOwnProperty, J = 1, he = 2, ue = 3, Z = 4, fi = 1, At = 2, ji = 3, St = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x84\x86-\x9F\uFFFE\uFFFF]|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?:[^\uD800-\uDBFF]|^)[\uDC00-\uDFFF]/, Tt = /[\x85\u2028\u2029]/, kt = /[,\[\]\{\}]/, fe = /^(?:!|!!|![a-z\-]+!)$/i, _e = /^(?:!|[^,\[\]\{\}])(?:%[0-9a-f]{2}|[0-9a-z\-#;\/\?:@&=\+\$,_\.!~\*'\(\)\[\]])*$/i;
function Li(i) {
  return Object.prototype.toString.call(i);
}
function S(i) {
  return i === 10 || i === 13;
}
function F(i) {
  return i === 9 || i === 32;
}
function A(i) {
  return i === 9 || i === 32 || i === 10 || i === 13;
}
function P(i) {
  return i === 44 || i === 91 || i === 93 || i === 123 || i === 125;
}
function Et(i) {
  var e;
  return 48 <= i && i <= 57 ? i - 48 : (e = i | 32, 97 <= e && e <= 102 ? e - 97 + 10 : -1);
}
function Nt(i) {
  return i === 120 ? 2 : i === 117 ? 4 : i === 85 ? 8 : 0;
}
function Ot(i) {
  return 48 <= i && i <= 57 ? i - 48 : -1;
}
function Di(i) {
  return i === 48 ? "\0" : i === 97 ? "\x07" : i === 98 ? "\b" : i === 116 || i === 9 ? "	" : i === 110 ? `
` : i === 118 ? "\v" : i === 102 ? "\f" : i === 114 ? "\r" : i === 101 ? "\x1B" : i === 32 ? " " : i === 34 ? '"' : i === 47 ? "/" : i === 92 ? "\\" : i === 78 ? "" : i === 95 ? " " : i === 76 ? "\u2028" : i === 80 ? "\u2029" : "";
}
function Ft(i) {
  return i <= 65535 ? String.fromCharCode(i) : String.fromCharCode(
    (i - 65536 >> 10) + 55296,
    (i - 65536 & 1023) + 56320
  );
}
var pe = new Array(256), de = new Array(256);
for (var I = 0; I < 256; I++)
  pe[I] = Di(I) ? 1 : 0, de[I] = Di(I);
function $t(i, e) {
  this.input = i, this.filename = e.filename || null, this.schema = e.schema || Ct, this.onWarning = e.onWarning || null, this.legacy = e.legacy || !1, this.json = e.json || !1, this.listener = e.listener || null, this.implicitTypes = this.schema.compiledImplicit, this.typeMap = this.schema.compiledTypeMap, this.length = i.length, this.position = 0, this.line = 0, this.lineStart = 0, this.lineIndent = 0, this.firstTabInLine = -1, this.documents = [];
}
function xe(i, e) {
  var n = {
    name: i.filename,
    buffer: i.input.slice(0, -1),
    // omit trailing \0
    position: i.position,
    line: i.line,
    column: i.position - i.lineStart
  };
  return n.snippet = yn(n), new T(e, n);
}
function _(i, e) {
  throw xe(i, e);
}
function Q(i, e) {
  i.onWarning && i.onWarning.call(null, xe(i, e));
}
var Ri = {
  YAML: function(e, n, t) {
    var r, s, o;
    e.version !== null && _(e, "duplication of %YAML directive"), t.length !== 1 && _(e, "YAML directive accepts exactly one argument"), r = /^([0-9]+)\.([0-9]+)$/.exec(t[0]), r === null && _(e, "ill-formed argument of the YAML directive"), s = parseInt(r[1], 10), o = parseInt(r[2], 10), s !== 1 && _(e, "unacceptable YAML version of the document"), e.version = t[0], e.checkLineBreaks = o < 2, o !== 1 && o !== 2 && Q(e, "unsupported YAML version of the document");
  },
  TAG: function(e, n, t) {
    var r, s;
    t.length !== 2 && _(e, "TAG directive accepts exactly two arguments"), r = t[0], s = t[1], fe.test(r) || _(e, "ill-formed tag handle (first argument) of the TAG directive"), N.call(e.tagMap, r) && _(e, 'there is a previously declared suffix for "' + r + '" tag handle'), _e.test(s) || _(e, "ill-formed tag prefix (second argument) of the TAG directive");
    try {
      s = decodeURIComponent(s);
    } catch {
      _(e, "tag prefix is malformed: " + s);
    }
    e.tagMap[r] = s;
  }
};
function k(i, e, n, t) {
  var r, s, o, l;
  if (e < n) {
    if (l = i.input.slice(e, n), t)
      for (r = 0, s = l.length; r < s; r += 1)
        o = l.charCodeAt(r), o === 9 || 32 <= o && o <= 1114111 || _(i, "expected valid JSON character");
    else
      St.test(l) && _(i, "the stream contains non-printable characters");
    i.result += l;
  }
}
function zi(i, e, n, t) {
  var r, s, o, l;
  for (w.isObject(n) || _(i, "cannot merge mappings; the provided source object is unacceptable"), r = Object.keys(n), o = 0, l = r.length; o < l; o += 1)
    s = r[o], N.call(e, s) || (e[s] = n[s], t[s] = !0);
}
function j(i, e, n, t, r, s, o, l, a) {
  var h, f;
  if (Array.isArray(r))
    for (r = Array.prototype.slice.call(r), h = 0, f = r.length; h < f; h += 1)
      Array.isArray(r[h]) && _(i, "nested arrays are not supported inside keys"), typeof r == "object" && Li(r[h]) === "[object Object]" && (r[h] = "[object Object]");
  if (typeof r == "object" && Li(r) === "[object Object]" && (r = "[object Object]"), r = String(r), e === null && (e = {}), t === "tag:yaml.org,2002:merge")
    if (Array.isArray(s))
      for (h = 0, f = s.length; h < f; h += 1)
        zi(i, e, s[h], n);
    else
      zi(i, e, s, n);
  else
    !i.json && !N.call(n, r) && N.call(e, r) && (i.line = o || i.line, i.lineStart = l || i.lineStart, i.position = a || i.position, _(i, "duplicated mapping key")), r === "__proto__" ? Object.defineProperty(e, r, {
      configurable: !0,
      enumerable: !0,
      writable: !0,
      value: s
    }) : e[r] = s, delete n[r];
  return e;
}
function Ai(i) {
  var e;
  e = i.input.charCodeAt(i.position), e === 10 ? i.position++ : e === 13 ? (i.position++, i.input.charCodeAt(i.position) === 10 && i.position++) : _(i, "a line break is expected"), i.line += 1, i.lineStart = i.position, i.firstTabInLine = -1;
}
function v(i, e, n) {
  for (var t = 0, r = i.input.charCodeAt(i.position); r !== 0; ) {
    for (; F(r); )
      r === 9 && i.firstTabInLine === -1 && (i.firstTabInLine = i.position), r = i.input.charCodeAt(++i.position);
    if (e && r === 35)
      do
        r = i.input.charCodeAt(++i.position);
      while (r !== 10 && r !== 13 && r !== 0);
    if (S(r))
      for (Ai(i), r = i.input.charCodeAt(i.position), t++, i.lineIndent = 0; r === 32; )
        i.lineIndent++, r = i.input.charCodeAt(++i.position);
    else
      break;
  }
  return n !== -1 && t !== 0 && i.lineIndent < n && Q(i, "deficient indentation"), t;
}
function si(i) {
  var e = i.position, n;
  return n = i.input.charCodeAt(e), !!((n === 45 || n === 46) && n === i.input.charCodeAt(e + 1) && n === i.input.charCodeAt(e + 2) && (e += 3, n = i.input.charCodeAt(e), n === 0 || A(n)));
}
function Si(i, e) {
  e === 1 ? i.result += " " : e > 1 && (i.result += w.repeat(`
`, e - 1));
}
function Mt(i, e, n) {
  var t, r, s, o, l, a, h, f, c = i.kind, p = i.result, u;
  if (u = i.input.charCodeAt(i.position), A(u) || P(u) || u === 35 || u === 38 || u === 42 || u === 33 || u === 124 || u === 62 || u === 39 || u === 34 || u === 37 || u === 64 || u === 96 || (u === 63 || u === 45) && (r = i.input.charCodeAt(i.position + 1), A(r) || n && P(r)))
    return !1;
  for (i.kind = "scalar", i.result = "", s = o = i.position, l = !1; u !== 0; ) {
    if (u === 58) {
      if (r = i.input.charCodeAt(i.position + 1), A(r) || n && P(r))
        break;
    } else if (u === 35) {
      if (t = i.input.charCodeAt(i.position - 1), A(t))
        break;
    } else {
      if (i.position === i.lineStart && si(i) || n && P(u))
        break;
      if (S(u))
        if (a = i.line, h = i.lineStart, f = i.lineIndent, v(i, !1, -1), i.lineIndent >= e) {
          l = !0, u = i.input.charCodeAt(i.position);
          continue;
        } else {
          i.position = o, i.line = a, i.lineStart = h, i.lineIndent = f;
          break;
        }
    }
    l && (k(i, s, o, !1), Si(i, i.line - a), s = o = i.position, l = !1), F(u) || (o = i.position + 1), u = i.input.charCodeAt(++i.position);
  }
  return k(i, s, o, !1), i.result ? !0 : (i.kind = c, i.result = p, !1);
}
function It(i, e) {
  var n, t, r;
  if (n = i.input.charCodeAt(i.position), n !== 39)
    return !1;
  for (i.kind = "scalar", i.result = "", i.position++, t = r = i.position; (n = i.input.charCodeAt(i.position)) !== 0; )
    if (n === 39)
      if (k(i, t, i.position, !0), n = i.input.charCodeAt(++i.position), n === 39)
        t = i.position, i.position++, r = i.position;
      else
        return !0;
    else
      S(n) ? (k(i, t, r, !0), Si(i, v(i, !1, e)), t = r = i.position) : i.position === i.lineStart && si(i) ? _(i, "unexpected end of the document within a single quoted scalar") : (i.position++, r = i.position);
  _(i, "unexpected end of the stream within a single quoted scalar");
}
function Pt(i, e) {
  var n, t, r, s, o, l;
  if (l = i.input.charCodeAt(i.position), l !== 34)
    return !1;
  for (i.kind = "scalar", i.result = "", i.position++, n = t = i.position; (l = i.input.charCodeAt(i.position)) !== 0; ) {
    if (l === 34)
      return k(i, n, i.position, !0), i.position++, !0;
    if (l === 92) {
      if (k(i, n, i.position, !0), l = i.input.charCodeAt(++i.position), S(l))
        v(i, !1, e);
      else if (l < 256 && pe[l])
        i.result += de[l], i.position++;
      else if ((o = Nt(l)) > 0) {
        for (r = o, s = 0; r > 0; r--)
          l = i.input.charCodeAt(++i.position), (o = Et(l)) >= 0 ? s = (s << 4) + o : _(i, "expected hexadecimal character");
        i.result += Ft(s), i.position++;
      } else
        _(i, "unknown escape sequence");
      n = t = i.position;
    } else
      S(l) ? (k(i, n, t, !0), Si(i, v(i, !1, e)), n = t = i.position) : i.position === i.lineStart && si(i) ? _(i, "unexpected end of the document within a double quoted scalar") : (i.position++, t = i.position);
  }
  _(i, "unexpected end of the stream within a double quoted scalar");
}
function jt(i, e) {
  var n = !0, t, r, s, o = i.tag, l, a = i.anchor, h, f, c, p, u, g = /* @__PURE__ */ Object.create(null), y, m, C, d;
  if (d = i.input.charCodeAt(i.position), d === 91)
    f = 93, u = !1, l = [];
  else if (d === 123)
    f = 125, u = !0, l = {};
  else
    return !1;
  for (i.anchor !== null && (i.anchorMap[i.anchor] = l), d = i.input.charCodeAt(++i.position); d !== 0; ) {
    if (v(i, !0, e), d = i.input.charCodeAt(i.position), d === f)
      return i.position++, i.tag = o, i.anchor = a, i.kind = u ? "mapping" : "sequence", i.result = l, !0;
    n ? d === 44 && _(i, "expected the node content, but found ','") : _(i, "missed comma between flow collection entries"), m = y = C = null, c = p = !1, d === 63 && (h = i.input.charCodeAt(i.position + 1), A(h) && (c = p = !0, i.position++, v(i, !0, e))), t = i.line, r = i.lineStart, s = i.position, L(i, e, J, !1, !0), m = i.tag, y = i.result, v(i, !0, e), d = i.input.charCodeAt(i.position), (p || i.line === t) && d === 58 && (c = !0, d = i.input.charCodeAt(++i.position), v(i, !0, e), L(i, e, J, !1, !0), C = i.result), u ? j(i, l, g, m, y, C, t, r, s) : c ? l.push(j(i, null, g, m, y, C, t, r, s)) : l.push(y), v(i, !0, e), d = i.input.charCodeAt(i.position), d === 44 ? (n = !0, d = i.input.charCodeAt(++i.position)) : n = !1;
  }
  _(i, "unexpected end of the stream within a flow collection");
}
function Lt(i, e) {
  var n, t, r = fi, s = !1, o = !1, l = e, a = 0, h = !1, f, c;
  if (c = i.input.charCodeAt(i.position), c === 124)
    t = !1;
  else if (c === 62)
    t = !0;
  else
    return !1;
  for (i.kind = "scalar", i.result = ""; c !== 0; )
    if (c = i.input.charCodeAt(++i.position), c === 43 || c === 45)
      fi === r ? r = c === 43 ? ji : At : _(i, "repeat of a chomping mode identifier");
    else if ((f = Ot(c)) >= 0)
      f === 0 ? _(i, "bad explicit indentation width of a block scalar; it cannot be less than one") : o ? _(i, "repeat of an indentation width identifier") : (l = e + f - 1, o = !0);
    else
      break;
  if (F(c)) {
    do
      c = i.input.charCodeAt(++i.position);
    while (F(c));
    if (c === 35)
      do
        c = i.input.charCodeAt(++i.position);
      while (!S(c) && c !== 0);
  }
  for (; c !== 0; ) {
    for (Ai(i), i.lineIndent = 0, c = i.input.charCodeAt(i.position); (!o || i.lineIndent < l) && c === 32; )
      i.lineIndent++, c = i.input.charCodeAt(++i.position);
    if (!o && i.lineIndent > l && (l = i.lineIndent), S(c)) {
      a++;
      continue;
    }
    if (i.lineIndent < l) {
      r === ji ? i.result += w.repeat(`
`, s ? 1 + a : a) : r === fi && s && (i.result += `
`);
      break;
    }
    for (t ? F(c) ? (h = !0, i.result += w.repeat(`
`, s ? 1 + a : a)) : h ? (h = !1, i.result += w.repeat(`
`, a + 1)) : a === 0 ? s && (i.result += " ") : i.result += w.repeat(`
`, a) : i.result += w.repeat(`
`, s ? 1 + a : a), s = !0, o = !0, a = 0, n = i.position; !S(c) && c !== 0; )
      c = i.input.charCodeAt(++i.position);
    k(i, n, i.position, !1);
  }
  return !0;
}
function Bi(i, e) {
  var n, t = i.tag, r = i.anchor, s = [], o, l = !1, a;
  if (i.firstTabInLine !== -1)
    return !1;
  for (i.anchor !== null && (i.anchorMap[i.anchor] = s), a = i.input.charCodeAt(i.position); a !== 0 && (i.firstTabInLine !== -1 && (i.position = i.firstTabInLine, _(i, "tab characters must not be used in indentation")), !(a !== 45 || (o = i.input.charCodeAt(i.position + 1), !A(o)))); ) {
    if (l = !0, i.position++, v(i, !0, -1) && i.lineIndent <= e) {
      s.push(null), a = i.input.charCodeAt(i.position);
      continue;
    }
    if (n = i.line, L(i, e, ue, !1, !0), s.push(i.result), v(i, !0, -1), a = i.input.charCodeAt(i.position), (i.line === n || i.lineIndent > e) && a !== 0)
      _(i, "bad indentation of a sequence entry");
    else if (i.lineIndent < e)
      break;
  }
  return l ? (i.tag = t, i.anchor = r, i.kind = "sequence", i.result = s, !0) : !1;
}
function Dt(i, e, n) {
  var t, r, s, o, l, a, h = i.tag, f = i.anchor, c = {}, p = /* @__PURE__ */ Object.create(null), u = null, g = null, y = null, m = !1, C = !1, d;
  if (i.firstTabInLine !== -1)
    return !1;
  for (i.anchor !== null && (i.anchorMap[i.anchor] = c), d = i.input.charCodeAt(i.position); d !== 0; ) {
    if (!m && i.firstTabInLine !== -1 && (i.position = i.firstTabInLine, _(i, "tab characters must not be used in indentation")), t = i.input.charCodeAt(i.position + 1), s = i.line, (d === 63 || d === 58) && A(t))
      d === 63 ? (m && (j(i, c, p, u, g, null, o, l, a), u = g = y = null), C = !0, m = !0, r = !0) : m ? (m = !1, r = !0) : _(i, "incomplete explicit mapping pair; a key node is missed; or followed by a non-tabulated empty line"), i.position += 1, d = t;
    else {
      if (o = i.line, l = i.lineStart, a = i.position, !L(i, n, he, !1, !0))
        break;
      if (i.line === s) {
        for (d = i.input.charCodeAt(i.position); F(d); )
          d = i.input.charCodeAt(++i.position);
        if (d === 58)
          d = i.input.charCodeAt(++i.position), A(d) || _(i, "a whitespace character is expected after the key-value separator within a block mapping"), m && (j(i, c, p, u, g, null, o, l, a), u = g = y = null), C = !0, m = !1, r = !1, u = i.tag, g = i.result;
        else if (C)
          _(i, "can not read an implicit mapping pair; a colon is missed");
        else
          return i.tag = h, i.anchor = f, !0;
      } else if (C)
        _(i, "can not read a block mapping entry; a multiline key may not be an implicit key");
      else
        return i.tag = h, i.anchor = f, !0;
    }
    if ((i.line === s || i.lineIndent > e) && (m && (o = i.line, l = i.lineStart, a = i.position), L(i, e, Z, !0, r) && (m ? g = i.result : y = i.result), m || (j(i, c, p, u, g, y, o, l, a), u = g = y = null), v(i, !0, -1), d = i.input.charCodeAt(i.position)), (i.line === s || i.lineIndent > e) && d !== 0)
      _(i, "bad indentation of a mapping entry");
    else if (i.lineIndent < e)
      break;
  }
  return m && j(i, c, p, u, g, null, o, l, a), C && (i.tag = h, i.anchor = f, i.kind = "mapping", i.result = c), C;
}
function Rt(i) {
  var e, n = !1, t = !1, r, s, o;
  if (o = i.input.charCodeAt(i.position), o !== 33)
    return !1;
  if (i.tag !== null && _(i, "duplication of a tag property"), o = i.input.charCodeAt(++i.position), o === 60 ? (n = !0, o = i.input.charCodeAt(++i.position)) : o === 33 ? (t = !0, r = "!!", o = i.input.charCodeAt(++i.position)) : r = "!", e = i.position, n) {
    do
      o = i.input.charCodeAt(++i.position);
    while (o !== 0 && o !== 62);
    i.position < i.length ? (s = i.input.slice(e, i.position), o = i.input.charCodeAt(++i.position)) : _(i, "unexpected end of the stream within a verbatim tag");
  } else {
    for (; o !== 0 && !A(o); )
      o === 33 && (t ? _(i, "tag suffix cannot contain exclamation marks") : (r = i.input.slice(e - 1, i.position + 1), fe.test(r) || _(i, "named tag handle cannot contain such characters"), t = !0, e = i.position + 1)), o = i.input.charCodeAt(++i.position);
    s = i.input.slice(e, i.position), kt.test(s) && _(i, "tag suffix cannot contain flow indicator characters");
  }
  s && !_e.test(s) && _(i, "tag name cannot contain such characters: " + s);
  try {
    s = decodeURIComponent(s);
  } catch {
    _(i, "tag name is malformed: " + s);
  }
  return n ? i.tag = s : N.call(i.tagMap, r) ? i.tag = i.tagMap[r] + s : r === "!" ? i.tag = "!" + s : r === "!!" ? i.tag = "tag:yaml.org,2002:" + s : _(i, 'undeclared tag handle "' + r + '"'), !0;
}
function zt(i) {
  var e, n;
  if (n = i.input.charCodeAt(i.position), n !== 38)
    return !1;
  for (i.anchor !== null && _(i, "duplication of an anchor property"), n = i.input.charCodeAt(++i.position), e = i.position; n !== 0 && !A(n) && !P(n); )
    n = i.input.charCodeAt(++i.position);
  return i.position === e && _(i, "name of an anchor node must contain at least one character"), i.anchor = i.input.slice(e, i.position), !0;
}
function Bt(i) {
  var e, n, t;
  if (t = i.input.charCodeAt(i.position), t !== 42)
    return !1;
  for (t = i.input.charCodeAt(++i.position), e = i.position; t !== 0 && !A(t) && !P(t); )
    t = i.input.charCodeAt(++i.position);
  return i.position === e && _(i, "name of an alias node must contain at least one character"), n = i.input.slice(e, i.position), N.call(i.anchorMap, n) || _(i, 'unidentified alias "' + n + '"'), i.result = i.anchorMap[n], v(i, !0, -1), !0;
}
function L(i, e, n, t, r) {
  var s, o, l, a = 1, h = !1, f = !1, c, p, u, g, y, m;
  if (i.listener !== null && i.listener("open", i), i.tag = null, i.anchor = null, i.kind = null, i.result = null, s = o = l = Z === n || ue === n, t && v(i, !0, -1) && (h = !0, i.lineIndent > e ? a = 1 : i.lineIndent === e ? a = 0 : i.lineIndent < e && (a = -1)), a === 1)
    for (; Rt(i) || zt(i); )
      v(i, !0, -1) ? (h = !0, l = s, i.lineIndent > e ? a = 1 : i.lineIndent === e ? a = 0 : i.lineIndent < e && (a = -1)) : l = !1;
  if (l && (l = h || r), (a === 1 || Z === n) && (J === n || he === n ? y = e : y = e + 1, m = i.position - i.lineStart, a === 1 ? l && (Bi(i, m) || Dt(i, m, y)) || jt(i, y) ? f = !0 : (o && Lt(i, y) || It(i, y) || Pt(i, y) ? f = !0 : Bt(i) ? (f = !0, (i.tag !== null || i.anchor !== null) && _(i, "alias node should not have any properties")) : Mt(i, y, J === n) && (f = !0, i.tag === null && (i.tag = "?")), i.anchor !== null && (i.anchorMap[i.anchor] = i.result)) : a === 0 && (f = l && Bi(i, m))), i.tag === null)
    i.anchor !== null && (i.anchorMap[i.anchor] = i.result);
  else if (i.tag === "?") {
    for (i.result !== null && i.kind !== "scalar" && _(i, 'unacceptable node kind for !<?> tag; it should be "scalar", not "' + i.kind + '"'), c = 0, p = i.implicitTypes.length; c < p; c += 1)
      if (g = i.implicitTypes[c], g.resolve(i.result)) {
        i.result = g.construct(i.result), i.tag = g.tag, i.anchor !== null && (i.anchorMap[i.anchor] = i.result);
        break;
      }
  } else if (i.tag !== "!") {
    if (N.call(i.typeMap[i.kind || "fallback"], i.tag))
      g = i.typeMap[i.kind || "fallback"][i.tag];
    else
      for (g = null, u = i.typeMap.multi[i.kind || "fallback"], c = 0, p = u.length; c < p; c += 1)
        if (i.tag.slice(0, u[c].tag.length) === u[c].tag) {
          g = u[c];
          break;
        }
    g || _(i, "unknown tag !<" + i.tag + ">"), i.result !== null && g.kind !== i.kind && _(i, "unacceptable node kind for !<" + i.tag + '> tag; it should be "' + g.kind + '", not "' + i.kind + '"'), g.resolve(i.result, i.tag) ? (i.result = g.construct(i.result, i.tag), i.anchor !== null && (i.anchorMap[i.anchor] = i.result)) : _(i, "cannot resolve a node with !<" + i.tag + "> explicit tag");
  }
  return i.listener !== null && i.listener("close", i), i.tag !== null || i.anchor !== null || f;
}
function Yt(i) {
  var e = i.position, n, t, r, s = !1, o;
  for (i.version = null, i.checkLineBreaks = i.legacy, i.tagMap = /* @__PURE__ */ Object.create(null), i.anchorMap = /* @__PURE__ */ Object.create(null); (o = i.input.charCodeAt(i.position)) !== 0 && (v(i, !0, -1), o = i.input.charCodeAt(i.position), !(i.lineIndent > 0 || o !== 37)); ) {
    for (s = !0, o = i.input.charCodeAt(++i.position), n = i.position; o !== 0 && !A(o); )
      o = i.input.charCodeAt(++i.position);
    for (t = i.input.slice(n, i.position), r = [], t.length < 1 && _(i, "directive name must not be less than one character in length"); o !== 0; ) {
      for (; F(o); )
        o = i.input.charCodeAt(++i.position);
      if (o === 35) {
        do
          o = i.input.charCodeAt(++i.position);
        while (o !== 0 && !S(o));
        break;
      }
      if (S(o))
        break;
      for (n = i.position; o !== 0 && !A(o); )
        o = i.input.charCodeAt(++i.position);
      r.push(i.input.slice(n, i.position));
    }
    o !== 0 && Ai(i), N.call(Ri, t) ? Ri[t](i, t, r) : Q(i, 'unknown document directive "' + t + '"');
  }
  if (v(i, !0, -1), i.lineIndent === 0 && i.input.charCodeAt(i.position) === 45 && i.input.charCodeAt(i.position + 1) === 45 && i.input.charCodeAt(i.position + 2) === 45 ? (i.position += 3, v(i, !0, -1)) : s && _(i, "directives end mark is expected"), L(i, i.lineIndent - 1, Z, !1, !0), v(i, !0, -1), i.checkLineBreaks && Tt.test(i.input.slice(e, i.position)) && Q(i, "non-ASCII line breaks are interpreted as content"), i.documents.push(i.result), i.position === i.lineStart && si(i)) {
    i.input.charCodeAt(i.position) === 46 && (i.position += 3, v(i, !0, -1));
    return;
  }
  if (i.position < i.length - 1)
    _(i, "end of the stream or a document separator is expected");
  else
    return;
}
function ge(i, e) {
  i = String(i), e = e || {}, i.length !== 0 && (i.charCodeAt(i.length - 1) !== 10 && i.charCodeAt(i.length - 1) !== 13 && (i += `
`), i.charCodeAt(0) === 65279 && (i = i.slice(1)));
  var n = new $t(i, e), t = i.indexOf("\0");
  for (t !== -1 && (n.position = t, _(n, "null byte is not allowed in input")), n.input += "\0"; n.input.charCodeAt(n.position) === 32; )
    n.lineIndent += 1, n.position += 1;
  for (; n.position < n.length - 1; )
    Yt(n);
  return n.documents;
}
function Wt(i, e, n) {
  e !== null && typeof e == "object" && typeof n > "u" && (n = e, e = null);
  var t = ge(i, n);
  if (typeof e != "function")
    return t;
  for (var r = 0, s = t.length; r < s; r += 1)
    e(t[r]);
}
function Ut(i, e) {
  var n = ge(i, e);
  if (n.length !== 0) {
    if (n.length === 1)
      return n[0];
    throw new T("expected a single document in the stream, but found more");
  }
}
var Ht = Wt, Gt = Ut, qt = {
  loadAll: Ht,
  load: Gt
}, Kt = le, Vt = qt.load;
const me = /^-{3}\s*[\n\r](.*?)[\n\r]-{3}\s*[\n\r]+/s;
function os(i, e) {
  var t, r;
  const n = i.match(me);
  if (n) {
    const s = Vt(n[1], {
      // To keep things simple, only allow strings, arrays, and plain objects.
      // https://www.yaml.org/spec/1.2/spec.html#id2802346
      schema: Kt
    });
    return s != null && s.title && ((t = e.setDiagramTitle) == null || t.call(e, s.title)), s != null && s.displayMode && ((r = e.setDisplayMode) == null || r.call(e, s.displayMode)), i.slice(n[0].length);
  } else
    return i;
}
const Xt = function(i, e) {
  for (let n of e)
    i.attr(n[0], n[1]);
}, Jt = function(i, e, n) {
  let t = /* @__PURE__ */ new Map();
  return n ? (t.set("width", "100%"), t.set("style", `max-width: ${e}px;`)) : (t.set("height", i), t.set("width", e)), t;
}, Zt = function(i, e, n, t) {
  const r = Jt(e, n, t);
  Xt(i, r);
}, Qt = function(i, e, n, t) {
  const r = e.node().getBBox(), s = r.width, o = r.height;
  x.info(`SVG bounds: ${s}x${o}`, r);
  let l = 0, a = 0;
  x.info(`Graph bounds: ${l}x${a}`, i), l = s + n * 2, a = o + n * 2, x.info(`Calculated bounds: ${l}x${a}`), Zt(e, a, l, t);
  const h = `${r.x - n} ${r.y - n} ${r.width + 2 * n} ${r.height + 2 * n}`;
  e.attr("viewBox", h);
}, G = {}, ir = (i, e, n) => {
  let t = "";
  return i in G && G[i] ? t = G[i](n) : x.warn(`No theme found for ${i}`), ` & {
    font-family: ${n.fontFamily};
    font-size: ${n.fontSize};
    fill: ${n.textColor}
  }

  /* Classes common for multiple diagrams */

  & .error-icon {
    fill: ${n.errorBkgColor};
  }
  & .error-text {
    fill: ${n.errorTextColor};
    stroke: ${n.errorTextColor};
  }

  & .edge-thickness-normal {
    stroke-width: 2px;
  }
  & .edge-thickness-thick {
    stroke-width: 3.5px
  }
  & .edge-pattern-solid {
    stroke-dasharray: 0;
  }

  & .edge-pattern-dashed{
    stroke-dasharray: 3;
  }
  .edge-pattern-dotted {
    stroke-dasharray: 2;
  }

  & .marker {
    fill: ${n.lineColor};
    stroke: ${n.lineColor};
  }
  & .marker.cross {
    stroke: ${n.lineColor};
  }

  & svg {
    font-family: ${n.fontFamily};
    font-size: ${n.fontSize};
  }

  ${t}

  ${e}
`;
}, er = (i, e) => {
  G[i] = e;
}, ss = ir;
let O = {};
const nr = function(i, e, n, t) {
  x.debug("parseDirective is being called", e, n, t);
  try {
    if (e !== void 0)
      switch (e = e.trim(), n) {
        case "open_directive":
          O = {};
          break;
        case "type_directive":
          if (!O)
            throw new Error("currentDirective is undefined");
          O.type = e.toLowerCase();
          break;
        case "arg_directive":
          if (!O)
            throw new Error("currentDirective is undefined");
          O.args = JSON.parse(e);
          break;
        case "close_directive":
          tr(i, O, t), O = void 0;
          break;
      }
  } catch (r) {
    x.error(
      `Error while rendering sequenceDiagram directive: ${e} jison context: ${n}`
    ), x.error(r.message);
  }
}, tr = function(i, e, n) {
  switch (x.info(`Directive type=${e.type} with args:`, e.args), e.type) {
    case "init":
    case "initialize": {
      ["config"].forEach((t) => {
        e.args[t] !== void 0 && (n === "flowchart-v2" && (n = "flowchart"), e.args[n] = e.args[t], delete e.args[t]);
      }), x.info("sanitize in handleDirective", e.args), W(e.args), x.info("sanitize in handleDirective (done)", e.args), Se(e.args);
      break;
    }
    case "wrap":
    case "nowrap":
      i && i.setWrap && i.setWrap(e.type === "wrap");
      break;
    case "themeCss":
      x.warn("themeCss encountered");
      break;
    default:
      x.warn(
        `Unhandled directive: source: '%%{${e.type}: ${JSON.stringify(
          e.args ? e.args : {}
        )}}%%`,
        e
      );
      break;
  }
}, rr = x, or = Te, ye = ke, sr = (i) => Ee(i, ye()), lr = Qt, ar = () => Ne, cr = (i, e, n, t) => nr(i, e, n, t), ii = {}, hr = (i, e, n) => {
  if (ii[i])
    throw new Error(`Diagram ${i} already registered.`);
  ii[i] = e, n && ve(i, n), er(i, e.styles), e.injectUtils && e.injectUtils(
    rr,
    or,
    ye,
    sr,
    lr,
    ar(),
    cr
  );
}, ur = (i) => {
  if (i in ii)
    return ii[i];
  throw new Error(`Diagram ${i} not found.`);
};
class fr extends Error {
  constructor(e) {
    super(e), this.name = "UnknownDiagramError";
  }
}
const _r = /%{2}{\s*(?:(\w+)\s*:|(\w+))\s*(?:(\w+)|((?:(?!}%{2}).|\r?\n)*))?\s*(?:}%{2})?/gi, pr = /\s*%%.*\n/gm, D = {}, dr = function(i, e) {
  i = i.replace(me, "").replace(_r, "").replace(pr, `
`);
  for (const [n, { detector: t }] of Object.entries(D))
    if (t(i, e))
      return n;
  throw new fr(
    `No diagram type detected matching given configuration for text: ${i}`
  );
}, ls = (...i) => {
  for (const { id: e, detector: n, loader: t } of i)
    ve(e, n, t);
}, as = async () => {
  x.debug("Loading registered diagrams");
  const e = (await Promise.allSettled(
    Object.entries(D).map(async ([n, { detector: t, loader: r }]) => {
      if (r)
        try {
          ur(n);
        } catch {
          try {
            const { diagram: o, id: l } = await r();
            hr(l, o, t);
          } catch (o) {
            throw x.error(`Failed to load external diagram with key ${n}. Removing from detectors.`), delete D[n], o;
          }
        }
    })
  )).filter((n) => n.status === "rejected");
  if (e.length > 0) {
    x.error(`Failed to load ${e.length} external diagrams`);
    for (const n of e)
      x.error(n);
    throw new Error(`Failed to load ${e.length} external diagrams`);
  }
}, ve = (i, e, n) => {
  D[i] ? x.error(`Detector with key ${i} already exists`) : D[i] = { detector: e, loader: n }, x.debug(`Detector with key ${i} added${n ? " with loader" : ""}`);
}, cs = (i) => D[i].loader;
var xr = typeof global == "object" && global && global.Object === Object && global;
const gr = xr;
var mr = typeof self == "object" && self && self.Object === Object && self, yr = gr || mr || Function("return this")();
const Ti = yr;
var vr = Ti.Symbol;
const ei = vr;
var be = Object.prototype, br = be.hasOwnProperty, wr = be.toString, z = ei ? ei.toStringTag : void 0;
function Cr(i) {
  var e = br.call(i, z), n = i[z];
  try {
    i[z] = void 0;
    var t = !0;
  } catch {
  }
  var r = wr.call(i);
  return t && (e ? i[z] = n : delete i[z]), r;
}
var Ar = Object.prototype, Sr = Ar.toString;
function Tr(i) {
  return Sr.call(i);
}
var kr = "[object Null]", Er = "[object Undefined]", Yi = ei ? ei.toStringTag : void 0;
function Nr(i) {
  return i == null ? i === void 0 ? Er : kr : Yi && Yi in Object(i) ? Cr(i) : Tr(i);
}
function we(i) {
  var e = typeof i;
  return i != null && (e == "object" || e == "function");
}
var Or = "[object AsyncFunction]", Fr = "[object Function]", $r = "[object GeneratorFunction]", Mr = "[object Proxy]";
function Ir(i) {
  if (!we(i))
    return !1;
  var e = Nr(i);
  return e == Fr || e == $r || e == Or || e == Mr;
}
var Pr = Ti["__core-js_shared__"];
const _i = Pr;
var Wi = function() {
  var i = /[^.]+$/.exec(_i && _i.keys && _i.keys.IE_PROTO || "");
  return i ? "Symbol(src)_1." + i : "";
}();
function jr(i) {
  return !!Wi && Wi in i;
}
var Lr = Function.prototype, Dr = Lr.toString;
function Rr(i) {
  if (i != null) {
    try {
      return Dr.call(i);
    } catch {
    }
    try {
      return i + "";
    } catch {
    }
  }
  return "";
}
var zr = /[\\^$.*+?()[\]{}|]/g, Br = /^\[object .+?Constructor\]$/, Yr = Function.prototype, Wr = Object.prototype, Ur = Yr.toString, Hr = Wr.hasOwnProperty, Gr = RegExp(
  "^" + Ur.call(Hr).replace(zr, "\\$&").replace(/hasOwnProperty|(function).*?(?=\\\()| for .+?(?=\\\])/g, "$1.*?") + "$"
);
function qr(i) {
  if (!we(i) || jr(i))
    return !1;
  var e = Ir(i) ? Gr : Br;
  return e.test(Rr(i));
}
function Kr(i, e) {
  return i == null ? void 0 : i[e];
}
function Ce(i, e) {
  var n = Kr(i, e);
  return qr(n) ? n : void 0;
}
var Vr = Ce(Object, "create");
const Y = Vr;
function Xr() {
  this.__data__ = Y ? Y(null) : {}, this.size = 0;
}
function Jr(i) {
  var e = this.has(i) && delete this.__data__[i];
  return this.size -= e ? 1 : 0, e;
}
var Zr = "__lodash_hash_undefined__", Qr = Object.prototype, io = Qr.hasOwnProperty;
function eo(i) {
  var e = this.__data__;
  if (Y) {
    var n = e[i];
    return n === Zr ? void 0 : n;
  }
  return io.call(e, i) ? e[i] : void 0;
}
var no = Object.prototype, to = no.hasOwnProperty;
function ro(i) {
  var e = this.__data__;
  return Y ? e[i] !== void 0 : to.call(e, i);
}
var oo = "__lodash_hash_undefined__";
function so(i, e) {
  var n = this.__data__;
  return this.size += this.has(i) ? 0 : 1, n[i] = Y && e === void 0 ? oo : e, this;
}
function $(i) {
  var e = -1, n = i == null ? 0 : i.length;
  for (this.clear(); ++e < n; ) {
    var t = i[e];
    this.set(t[0], t[1]);
  }
}
$.prototype.clear = Xr;
$.prototype.delete = Jr;
$.prototype.get = eo;
$.prototype.has = ro;
$.prototype.set = so;
function lo() {
  this.__data__ = [], this.size = 0;
}
function ao(i, e) {
  return i === e || i !== i && e !== e;
}
function li(i, e) {
  for (var n = i.length; n--; )
    if (ao(i[n][0], e))
      return n;
  return -1;
}
var co = Array.prototype, ho = co.splice;
function uo(i) {
  var e = this.__data__, n = li(e, i);
  if (n < 0)
    return !1;
  var t = e.length - 1;
  return n == t ? e.pop() : ho.call(e, n, 1), --this.size, !0;
}
function fo(i) {
  var e = this.__data__, n = li(e, i);
  return n < 0 ? void 0 : e[n][1];
}
function _o(i) {
  return li(this.__data__, i) > -1;
}
function po(i, e) {
  var n = this.__data__, t = li(n, i);
  return t < 0 ? (++this.size, n.push([i, e])) : n[t][1] = e, this;
}
function R(i) {
  var e = -1, n = i == null ? 0 : i.length;
  for (this.clear(); ++e < n; ) {
    var t = i[e];
    this.set(t[0], t[1]);
  }
}
R.prototype.clear = lo;
R.prototype.delete = uo;
R.prototype.get = fo;
R.prototype.has = _o;
R.prototype.set = po;
var xo = Ce(Ti, "Map");
const go = xo;
function mo() {
  this.size = 0, this.__data__ = {
    hash: new $(),
    map: new (go || R)(),
    string: new $()
  };
}
function yo(i) {
  var e = typeof i;
  return e == "string" || e == "number" || e == "symbol" || e == "boolean" ? i !== "__proto__" : i === null;
}
function ai(i, e) {
  var n = i.__data__;
  return yo(e) ? n[typeof e == "string" ? "string" : "hash"] : n.map;
}
function vo(i) {
  var e = ai(this, i).delete(i);
  return this.size -= e ? 1 : 0, e;
}
function bo(i) {
  return ai(this, i).get(i);
}
function wo(i) {
  return ai(this, i).has(i);
}
function Co(i, e) {
  var n = ai(this, i), t = n.size;
  return n.set(i, e), this.size += n.size == t ? 0 : 1, this;
}
function M(i) {
  var e = -1, n = i == null ? 0 : i.length;
  for (this.clear(); ++e < n; ) {
    var t = i[e];
    this.set(t[0], t[1]);
  }
}
M.prototype.clear = mo;
M.prototype.delete = vo;
M.prototype.get = bo;
M.prototype.has = wo;
M.prototype.set = Co;
var Ao = "Expected a function";
function U(i, e) {
  if (typeof i != "function" || e != null && typeof e != "function")
    throw new TypeError(Ao);
  var n = function() {
    var t = arguments, r = e ? e.apply(this, t) : t[0], s = n.cache;
    if (s.has(r))
      return s.get(r);
    var o = i.apply(this, t);
    return n.cache = s.set(r, o) || s, o;
  };
  return n.cache = new (U.Cache || M)(), n;
}
U.Cache = M;
const So = {
  curveBasis: We,
  curveBasisClosed: Ue,
  curveBasisOpen: He,
  curveBumpX: Be,
  curveBumpY: Ye,
  curveBundle: Ge,
  curveCardinalClosed: Ke,
  curveCardinalOpen: Ve,
  curveCardinal: qe,
  curveCatmullRomClosed: Je,
  curveCatmullRomOpen: Ze,
  curveCatmullRom: Xe,
  curveLinear: ze,
  curveLinearClosed: Qe,
  curveMonotoneX: en,
  curveMonotoneY: nn,
  curveNatural: tn,
  curveStep: rn,
  curveStepAfter: sn,
  curveStepBefore: on
}, pi = /%{2}{\s*(?:(\w+)\s*:|(\w+))\s*(?:(\w+)|((?:(?!}%{2}).|\r?\n)*))?\s*(?:}%{2})?/gi, To = /\s*(?:(\w+)(?=:):|(\w+))\s*(?:(\w+)|((?:(?!}%{2}).|\r?\n)*))?\s*(?:}%{2})?/gi, ko = function(i, e) {
  const n = Ae(i, /(?:init\b)|(?:initialize\b)/);
  let t = {};
  if (Array.isArray(n)) {
    const r = n.map((s) => s.args);
    W(r), t = Hi(t, [...r]);
  } else
    t = n.args;
  if (t) {
    let r = dr(i, e);
    ["config"].forEach((s) => {
      t[s] !== void 0 && (r === "flowchart-v2" && (r = "flowchart"), t[r] = t[s], delete t[s]);
    });
  }
  return t;
}, Ae = function(i, e = null) {
  try {
    const n = new RegExp(
      `[%]{2}(?![{]${To.source})(?=[}][%]{2}).*
`,
      "ig"
    );
    i = i.trim().replace(n, "").replace(/'/gm, '"'), x.debug(
      `Detecting diagram directive${e !== null ? " type:" + e : ""} based on the text:${i}`
    );
    let t;
    const r = [];
    for (; (t = pi.exec(i)) !== null; )
      if (t.index === pi.lastIndex && pi.lastIndex++, t && !e || e && t[1] && t[1].match(e) || e && t[2] && t[2].match(e)) {
        const s = t[1] ? t[1] : t[2], o = t[3] ? t[3].trim() : t[4] ? JSON.parse(t[4].trim()) : null;
        r.push({ type: s, args: o });
      }
    return r.length === 0 && r.push({ type: i, args: null }), r.length === 1 ? r[0] : r;
  } catch (n) {
    return x.error(
      `ERROR: ${n.message} - Unable to parse directive
      ${e !== null ? " type:" + e : ""} based on the text:${i}`
    ), { type: null, args: null };
  }
}, Eo = function(i, e) {
  for (const [n, t] of e.entries())
    if (t.match(i))
      return n;
  return -1;
};
function No(i, e) {
  if (!i)
    return e;
  const n = `curve${i.charAt(0).toUpperCase() + i.slice(1)}`;
  return So[n] || e;
}
function Oo(i, e) {
  const n = i.trim();
  if (n)
    return e.securityLevel !== "loose" ? Gi(n) : n;
}
const Fo = (i, ...e) => {
  const n = i.split("."), t = n.length - 1, r = n[t];
  let s = window;
  for (let o = 0; o < t; o++)
    if (s = s[n[o]], !s)
      return;
  s[r](...e);
};
function ni(i, e) {
  return i && e ? Math.sqrt(Math.pow(e.x - i.x, 2) + Math.pow(e.y - i.y, 2)) : 0;
}
function $o(i) {
  let e, n = 0;
  i.forEach((s) => {
    n += ni(s, e), e = s;
  });
  let t = n / 2, r;
  return e = void 0, i.forEach((s) => {
    if (e && !r) {
      const o = ni(s, e);
      if (o < t)
        t -= o;
      else {
        const l = t / o;
        l <= 0 && (r = e), l >= 1 && (r = { x: s.x, y: s.y }), l > 0 && l < 1 && (r = {
          x: (1 - l) * e.x + l * s.x,
          y: (1 - l) * e.y + l * s.y
        });
      }
    }
    e = s;
  }), r;
}
function Mo(i) {
  return i.length === 1 ? i[0] : $o(i);
}
const Io = (i, e, n) => {
  let t;
  x.info(`our points ${JSON.stringify(e)}`), e[0] !== n && (e = e.reverse());
  let s = 25, o;
  t = void 0, e.forEach((f) => {
    if (t && !o) {
      const c = ni(f, t);
      if (c < s)
        s -= c;
      else {
        const p = s / c;
        p <= 0 && (o = t), p >= 1 && (o = { x: f.x, y: f.y }), p > 0 && p < 1 && (o = {
          x: (1 - p) * t.x + p * f.x,
          y: (1 - p) * t.y + p * f.y
        });
      }
    }
    t = f;
  });
  const l = i ? 10 : 5, a = Math.atan2(e[0].y - o.y, e[0].x - o.x), h = { x: 0, y: 0 };
  return h.x = Math.sin(a) * l + (e[0].x + o.x) / 2, h.y = -Math.cos(a) * l + (e[0].y + o.y) / 2, h;
};
function Po(i, e, n) {
  let t = JSON.parse(JSON.stringify(n)), r;
  x.info("our points", t), e !== "start_left" && e !== "start_right" && (t = t.reverse()), t.forEach((c) => {
    r = c;
  });
  let o = 25 + i, l;
  r = void 0, t.forEach((c) => {
    if (r && !l) {
      const p = ni(c, r);
      if (p < o)
        o -= p;
      else {
        const u = o / p;
        u <= 0 && (l = r), u >= 1 && (l = { x: c.x, y: c.y }), u > 0 && u < 1 && (l = {
          x: (1 - u) * r.x + u * c.x,
          y: (1 - u) * r.y + u * c.y
        });
      }
    }
    r = c;
  });
  const a = 10 + i * 0.5, h = Math.atan2(t[0].y - l.y, t[0].x - l.x), f = { x: 0, y: 0 };
  return f.x = Math.sin(h) * a + (t[0].x + l.x) / 2, f.y = -Math.cos(h) * a + (t[0].y + l.y) / 2, e === "start_left" && (f.x = Math.sin(h + Math.PI) * a + (t[0].x + l.x) / 2, f.y = -Math.cos(h + Math.PI) * a + (t[0].y + l.y) / 2), e === "end_right" && (f.x = Math.sin(h - Math.PI) * a + (t[0].x + l.x) / 2 - 5, f.y = -Math.cos(h - Math.PI) * a + (t[0].y + l.y) / 2 - 5), e === "end_left" && (f.x = Math.sin(h) * a + (t[0].x + l.x) / 2 - 5, f.y = -Math.cos(h) * a + (t[0].y + l.y) / 2 - 5), f;
}
function jo(i) {
  let e = "", n = "";
  for (const t of i)
    t !== void 0 && (t.startsWith("color:") || t.startsWith("text-align:") ? n = n + t + ";" : e = e + t + ";");
  return { style: e, labelStyle: n };
}
let Ui = 0;
const Lo = () => (Ui++, "id-" + Math.random().toString(36).substr(2, 12) + "-" + Ui);
function Do(i) {
  let e = "";
  const n = "0123456789abcdef", t = n.length;
  for (let r = 0; r < i; r++)
    e += n.charAt(Math.floor(Math.random() * t));
  return e;
}
const Ro = (i) => Do(i.length), zo = function() {
  return {
    x: 0,
    y: 0,
    fill: void 0,
    anchor: "start",
    style: "#666",
    width: 100,
    height: 100,
    textMargin: 0,
    rx: 0,
    ry: 0,
    valign: void 0
  };
}, Bo = function(i, e) {
  const n = e.text.replace(xi.lineBreakRegex, " "), [, t] = Ei(e.fontSize), r = i.append("text");
  r.attr("x", e.x), r.attr("y", e.y), r.style("text-anchor", e.anchor), r.style("font-family", e.fontFamily), r.style("font-size", t), r.style("font-weight", e.fontWeight), r.attr("fill", e.fill), e.class !== void 0 && r.attr("class", e.class);
  const s = r.append("tspan");
  return s.attr("x", e.x + e.textMargin * 2), s.attr("fill", e.fill), s.text(n), r;
}, Yo = U(
  (i, e, n) => {
    if (!i || (n = Object.assign(
      { fontSize: 12, fontWeight: 400, fontFamily: "Arial", joinWith: "<br/>" },
      n
    ), xi.lineBreakRegex.test(i)))
      return i;
    const t = i.split(" "), r = [];
    let s = "";
    return t.forEach((o, l) => {
      const a = ti(`${o} `, n), h = ti(s, n);
      if (a > e) {
        const { hyphenatedStrings: p, remainingWord: u } = Wo(o, e, "-", n);
        r.push(s, ...p), s = u;
      } else
        h + a >= e ? (r.push(s), s = o) : s = [s, o].filter(Boolean).join(" ");
      l + 1 === t.length && r.push(s);
    }), r.filter((o) => o !== "").join(n.joinWith);
  },
  (i, e, n) => `${i}${e}${n.fontSize}${n.fontWeight}${n.fontFamily}${n.joinWith}`
), Wo = U(
  (i, e, n = "-", t) => {
    t = Object.assign(
      { fontSize: 12, fontWeight: 400, fontFamily: "Arial", margin: 0 },
      t
    );
    const r = [...i], s = [];
    let o = "";
    return r.forEach((l, a) => {
      const h = `${o}${l}`;
      if (ti(h, t) >= e) {
        const c = a + 1, p = r.length === c, u = `${h}${n}`;
        s.push(p ? h : u), o = "";
      } else
        o = h;
    }), { hyphenatedStrings: s, remainingWord: o };
  },
  (i, e, n = "-", t) => `${i}${e}${n}${t.fontSize}${t.fontWeight}${t.fontFamily}`
);
function Uo(i, e) {
  return e = Object.assign(
    { fontSize: 12, fontWeight: 400, fontFamily: "Arial", margin: 15 },
    e
  ), ki(i, e).height;
}
function ti(i, e) {
  return e = Object.assign({ fontSize: 12, fontWeight: 400, fontFamily: "Arial" }, e), ki(i, e).width;
}
const ki = U(
  (i, e) => {
    e = Object.assign({ fontSize: 12, fontWeight: 400, fontFamily: "Arial" }, e);
    const { fontSize: n, fontFamily: t, fontWeight: r } = e;
    if (!i)
      return { width: 0, height: 0 };
    const [, s] = Ei(n), o = ["sans-serif", t], l = i.split(xi.lineBreakRegex), a = [], h = Fe("body");
    if (!h.remove)
      return { width: 0, height: 0, lineHeight: 0 };
    const f = h.append("svg");
    for (const p of o) {
      let u = 0;
      const g = { width: 0, height: 0, lineHeight: 0 };
      for (const y of l) {
        const m = zo();
        m.text = y;
        const C = Bo(f, m).style("font-size", s).style("font-weight", r).style("font-family", p), d = (C._groups || C)[0][0].getBBox();
        if (d.width === 0 && d.height === 0)
          throw new Error("svg element not in render tree");
        g.width = Math.round(Math.max(g.width, d.width)), u = Math.round(d.height), g.height += u, g.lineHeight = Math.round(Math.max(g.lineHeight, u));
      }
      a.push(g);
    }
    f.remove();
    const c = isNaN(a[1].height) || isNaN(a[1].width) || isNaN(a[1].lineHeight) || a[0].height > a[1].height && a[0].width > a[1].width && a[0].lineHeight > a[1].lineHeight ? 0 : 1;
    return a[c];
  },
  (i, e) => `${i}${e.fontSize}${e.fontWeight}${e.fontFamily}`
), Ho = class {
  constructor(e, n) {
    this.deterministic = e, this.seed = n, this.count = n ? n.length : 0;
  }
  next() {
    return this.deterministic ? this.count++ : Date.now();
  }
};
let H;
const Go = function(i) {
  return H = H || document.createElement("div"), i = escape(i).replace(/%26/g, "&").replace(/%23/g, "#").replace(/%3B/g, ";"), H.innerHTML = i, unescape(H.textContent);
}, W = (i) => {
  if (x.debug("directiveSanitizer called with", i), typeof i == "object" && (i.length ? i.forEach((e) => W(e)) : Object.keys(i).forEach((e) => {
    x.debug("Checking key", e), e.startsWith("__") && (x.debug("sanitize deleting __ option", e), delete i[e]), e.includes("proto") && (x.debug("sanitize deleting proto option", e), delete i[e]), e.includes("constr") && (x.debug("sanitize deleting constr option", e), delete i[e]), e.includes("themeCSS") && (x.debug("sanitizing themeCss option"), i[e] = q(i[e])), e.includes("fontFamily") && (x.debug("sanitizing fontFamily option"), i[e] = q(i[e])), e.includes("altFontFamily") && (x.debug("sanitizing altFontFamily option"), i[e] = q(i[e])), Oe.includes(e) ? typeof i[e] == "object" && (x.debug("sanitize deleting object", e), W(i[e])) : (x.debug("sanitize deleting option", e), delete i[e]);
  })), i.themeVariables) {
    const e = Object.keys(i.themeVariables);
    for (const n of e) {
      const t = i.themeVariables[n];
      t && t.match && !t.match(/^[\d "#%(),.;A-Za-z]+$/) && (i.themeVariables[n] = "");
    }
  }
  x.debug("After sanitization", i);
}, q = (i) => {
  let e = 0, n = 0;
  for (const t of i) {
    if (e < n)
      return "{ /* ERROR: Unbalanced CSS */ }";
    t === "{" ? e++ : t === "}" && n++;
  }
  return e !== n ? "{ /* ERROR: Unbalanced CSS */ }" : i;
};
function us(i) {
  return "str" in i;
}
function fs(i) {
  return i instanceof Error ? i.message : String(i);
}
const qo = (i, e, n, t) => {
  if (!t)
    return;
  const r = i.node().getBBox();
  i.append("text").text(t).attr("x", r.x + r.width / 2).attr("y", -n).attr("class", e);
}, Ei = (i) => {
  if (typeof i == "number")
    return [i, i + "px"];
  const e = parseInt(i, 10);
  return Number.isNaN(e) ? [void 0, void 0] : i === String(e) ? [e, i + "px"] : [e, i];
}, _s = {
  assignWithDepth: Hi,
  wrapLabel: Yo,
  calculateTextHeight: Uo,
  calculateTextWidth: ti,
  calculateTextDimensions: ki,
  detectInit: ko,
  detectDirective: Ae,
  isSubstringInArray: Eo,
  interpolateToCurve: No,
  calcLabelPosition: Mo,
  calcCardinalityPosition: Io,
  calcTerminalLabelPosition: Po,
  formatUrl: Oo,
  getStylesFromArray: jo,
  generateId: Lo,
  random: Ro,
  runFunc: Fo,
  entityDecode: Go,
  initIdGenerator: Ho,
  directiveSanitizer: W,
  sanitizeCss: q,
  insertTitle: qo,
  parseFontSize: Ei
};
export {
  Zo as $,
  No as A,
  Qt as B,
  we as C,
  ao as D,
  U as E,
  M as F,
  We as G,
  Ro as H,
  ye as I,
  lr as J,
  ns as K,
  R as L,
  go as M,
  Ei as N,
  mi as O,
  Jo as P,
  is as Q,
  Oi as R,
  ei as S,
  Ni as T,
  fr as U,
  es as V,
  Qo as W,
  Vo as X,
  Xo as Y,
  rs as Z,
  ts as _,
  hr as a,
  Lo as a0,
  ur as b,
  cs as c,
  dr as d,
  os as e,
  Ce as f,
  fs as g,
  Ti as h,
  us as i,
  Nr as j,
  Ir as k,
  as as l,
  gr as m,
  W as n,
  ss as o,
  nr as p,
  ti as q,
  ls as r,
  Gi as s,
  Rr as t,
  _s as u,
  Zt as v,
  Yo as w,
  Uo as x,
  ze as y,
  jo as z
};
//# sourceMappingURL=utils-8ea37061.js.map
