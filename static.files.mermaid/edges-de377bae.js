import { l as u, m as R, g as w, h as v, b as z } from "./commonDb-41f8b4c5.js";
import { c as P } from "./createText-23817c58.js";
import { p as D } from "./mermaidAPI-67f627de.js";
import { p as Q } from "./svgDraw-0a992cdb.js";
import { u as U, G as V } from "./utils-8ea37061.js";
import { l as et } from "./line-05ccbb85.js";
const st = (r, t, e, n) => {
  t.forEach((a) => {
    bt[a](r, e, n);
  });
}, it = (r, t, e) => {
  u.trace("Making markers for ", e), r.append("defs").append("marker").attr("id", t + "-extensionStart").attr("class", "marker extension " + t).attr("refX", 0).attr("refY", 7).attr("markerWidth", 190).attr("markerHeight", 240).attr("orient", "auto").append("path").attr("d", "M 1,7 L18,13 V 1 Z"), r.append("defs").append("marker").attr("id", t + "-extensionEnd").attr("class", "marker extension " + t).attr("refX", 19).attr("refY", 7).attr("markerWidth", 20).attr("markerHeight", 28).attr("orient", "auto").append("path").attr("d", "M 1,1 V 13 L18,7 Z");
}, nt = (r, t) => {
  r.append("defs").append("marker").attr("id", t + "-compositionStart").attr("class", "marker composition " + t).attr("refX", 0).attr("refY", 7).attr("markerWidth", 190).attr("markerHeight", 240).attr("orient", "auto").append("path").attr("d", "M 18,7 L9,13 L1,7 L9,1 Z"), r.append("defs").append("marker").attr("id", t + "-compositionEnd").attr("class", "marker composition " + t).attr("refX", 19).attr("refY", 7).attr("markerWidth", 20).attr("markerHeight", 28).attr("orient", "auto").append("path").attr("d", "M 18,7 L9,13 L1,7 L9,1 Z");
}, lt = (r, t) => {
  r.append("defs").append("marker").attr("id", t + "-aggregationStart").attr("class", "marker aggregation " + t).attr("refX", 0).attr("refY", 7).attr("markerWidth", 190).attr("markerHeight", 240).attr("orient", "auto").append("path").attr("d", "M 18,7 L9,13 L1,7 L9,1 Z"), r.append("defs").append("marker").attr("id", t + "-aggregationEnd").attr("class", "marker aggregation " + t).attr("refX", 19).attr("refY", 7).attr("markerWidth", 20).attr("markerHeight", 28).attr("orient", "auto").append("path").attr("d", "M 18,7 L9,13 L1,7 L9,1 Z");
}, ct = (r, t) => {
  r.append("defs").append("marker").attr("id", t + "-dependencyStart").attr("class", "marker dependency " + t).attr("refX", 0).attr("refY", 7).attr("markerWidth", 190).attr("markerHeight", 240).attr("orient", "auto").append("path").attr("d", "M 5,7 L9,13 L1,7 L9,1 Z"), r.append("defs").append("marker").attr("id", t + "-dependencyEnd").attr("class", "marker dependency " + t).attr("refX", 19).attr("refY", 7).attr("markerWidth", 20).attr("markerHeight", 28).attr("orient", "auto").append("path").attr("d", "M 18,7 L9,13 L14,7 L9,1 Z");
}, ht = (r, t) => {
  r.append("defs").append("marker").attr("id", t + "-lollipopStart").attr("class", "marker lollipop " + t).attr("refX", 0).attr("refY", 7).attr("markerWidth", 190).attr("markerHeight", 240).attr("orient", "auto").append("circle").attr("stroke", "black").attr("fill", "white").attr("cx", 6).attr("cy", 7).attr("r", 6);
}, ot = (r, t) => {
  r.append("marker").attr("id", t + "-pointEnd").attr("class", "marker " + t).attr("viewBox", "0 0 12 20").attr("refX", 10).attr("refY", 5).attr("markerUnits", "userSpaceOnUse").attr("markerWidth", 12).attr("markerHeight", 12).attr("orient", "auto").append("path").attr("d", "M 0 0 L 10 5 L 0 10 z").attr("class", "arrowMarkerPath").style("stroke-width", 1).style("stroke-dasharray", "1,0"), r.append("marker").attr("id", t + "-pointStart").attr("class", "marker " + t).attr("viewBox", "0 0 10 10").attr("refX", 0).attr("refY", 5).attr("markerUnits", "userSpaceOnUse").attr("markerWidth", 12).attr("markerHeight", 12).attr("orient", "auto").append("path").attr("d", "M 0 5 L 10 10 L 10 0 z").attr("class", "arrowMarkerPath").style("stroke-width", 1).style("stroke-dasharray", "1,0");
}, ft = (r, t) => {
  r.append("marker").attr("id", t + "-circleEnd").attr("class", "marker " + t).attr("viewBox", "0 0 10 10").attr("refX", 11).attr("refY", 5).attr("markerUnits", "userSpaceOnUse").attr("markerWidth", 11).attr("markerHeight", 11).attr("orient", "auto").append("circle").attr("cx", "5").attr("cy", "5").attr("r", "5").attr("class", "arrowMarkerPath").style("stroke-width", 1).style("stroke-dasharray", "1,0"), r.append("marker").attr("id", t + "-circleStart").attr("class", "marker " + t).attr("viewBox", "0 0 10 10").attr("refX", -1).attr("refY", 5).attr("markerUnits", "userSpaceOnUse").attr("markerWidth", 11).attr("markerHeight", 11).attr("orient", "auto").append("circle").attr("cx", "5").attr("cy", "5").attr("r", "5").attr("class", "arrowMarkerPath").style("stroke-width", 1).style("stroke-dasharray", "1,0");
}, dt = (r, t) => {
  r.append("marker").attr("id", t + "-crossEnd").attr("class", "marker cross " + t).attr("viewBox", "0 0 11 11").attr("refX", 12).attr("refY", 5.2).attr("markerUnits", "userSpaceOnUse").attr("markerWidth", 11).attr("markerHeight", 11).attr("orient", "auto").append("path").attr("d", "M 1,1 l 9,9 M 10,1 l -9,9").attr("class", "arrowMarkerPath").style("stroke-width", 2).style("stroke-dasharray", "1,0"), r.append("marker").attr("id", t + "-crossStart").attr("class", "marker cross " + t).attr("viewBox", "0 0 11 11").attr("refX", -1).attr("refY", 5.2).attr("markerUnits", "userSpaceOnUse").attr("markerWidth", 11).attr("markerHeight", 11).attr("orient", "auto").append("path").attr("d", "M 1,1 l 9,9 M 10,1 l -9,9").attr("class", "arrowMarkerPath").style("stroke-width", 2).style("stroke-dasharray", "1,0");
}, pt = (r, t) => {
  r.append("defs").append("marker").attr("id", t + "-barbEnd").attr("refX", 19).attr("refY", 7).attr("markerWidth", 20).attr("markerHeight", 14).attr("markerUnits", "strokeWidth").attr("orient", "auto").append("path").attr("d", "M 19,7 L9,13 L14,7 L9,1 Z");
}, bt = {
  extension: it,
  composition: nt,
  aggregation: lt,
  dependency: ct,
  lollipop: ht,
  point: ot,
  circle: ft,
  cross: dt,
  barb: pt
}, Pt = st;
function xt(r, t) {
  t && r.attr("style", t);
}
function ut(r) {
  const t = v(document.createElementNS("http://www.w3.org/2000/svg", "foreignObject")), e = t.append("xhtml:div"), n = r.label, a = r.isNode ? "nodeLabel" : "edgeLabel";
  return e.html(
    '<span class="' + a + '" ' + (r.labelStyle ? 'style="' + r.labelStyle + '"' : "") + ">" + n + "</span>"
  ), xt(e, r.labelStyle), e.style("display", "inline-block"), e.style("white-space", "nowrap"), e.attr("xmlns", "http://www.w3.org/1999/xhtml"), t.node();
}
const yt = (r, t, e, n) => {
  let a = r || "";
  if (typeof a == "object" && (a = a[0]), R(w().flowchart.htmlLabels)) {
    a = a.replace(/\\n|\n/g, "<br />"), u.info("vertexText" + a);
    const s = {
      isNode: n,
      label: D(a).replace(
        /fa[blrs]?:fa-[\w-]+/g,
        (l) => `<i class='${l.replace(":", " ")}'></i>`
      ),
      labelStyle: t.replace("fill:", "color:")
    };
    return ut(s);
  } else {
    const s = document.createElementNS("http://www.w3.org/2000/svg", "text");
    s.setAttribute("style", t.replace("color:", "fill:"));
    let i = [];
    typeof a == "string" ? i = a.split(/\\n|\n|<br\s*\/?>/gi) : Array.isArray(a) ? i = a : i = [];
    for (const l of i) {
      const c = document.createElementNS("http://www.w3.org/2000/svg", "tspan");
      c.setAttributeNS("http://www.w3.org/XML/1998/namespace", "xml:space", "preserve"), c.setAttribute("dy", "1em"), c.setAttribute("x", "0"), e ? c.setAttribute("class", "title-row") : c.setAttribute("class", "row"), c.textContent = l.trim(), s.appendChild(c);
    }
    return s;
  }
}, T = yt, S = (r, t, e, n) => {
  let a;
  const s = t.useHtmlLabels || R(w().flowchart.htmlLabels);
  e ? a = e : a = "node default";
  const i = r.insert("g").attr("class", a).attr("id", t.domId || t.id), l = i.insert("g").attr("class", "label").attr("style", t.labelStyle);
  let c;
  t.labelText === void 0 ? c = "" : c = typeof t.labelText == "string" ? t.labelText : t.labelText[0];
  const o = l.node();
  let h;
  t.labelType === "markdown" ? h = P(l, z(D(c), w()), {
    useHtmlLabels: s,
    width: t.width || w().flowchart.wrappingWidth,
    classes: "markdown-node-label"
  }) : h = o.appendChild(
    T(
      z(D(c), w()),
      t.labelStyle,
      !1,
      n
    )
  );
  let f = h.getBBox();
  if (R(w().flowchart.htmlLabels)) {
    const d = h.children[0], p = v(h);
    f = d.getBoundingClientRect(), p.attr("width", f.width), p.attr("height", f.height);
  }
  const b = t.padding / 2;
  return s ? l.attr("transform", "translate(" + -f.width / 2 + ", " + -f.height / 2 + ")") : l.attr("transform", "translate(" + 0 + ", " + -f.height / 2 + ")"), t.centerLabel && l.attr("transform", "translate(" + -f.width / 2 + ", " + -f.height / 2 + ")"), l.insert("rect", ":first-child"), { shapeSvg: i, bbox: f, halfPadding: b, label: l };
}, m = (r, t) => {
  const e = t.node().getBBox();
  r.width = e.width, r.height = e.height;
};
function _(r, t, e, n) {
  return r.insert("polygon", ":first-child").attr(
    "points",
    n.map(function(a) {
      return a.x + "," + a.y;
    }).join(" ")
  ).attr("class", "label-container").attr("transform", "translate(" + -t / 2 + "," + e / 2 + ")");
}
function gt(r, t) {
  return r.intersect(t);
}
function tt(r, t, e, n) {
  var a = r.x, s = r.y, i = a - n.x, l = s - n.y, c = Math.sqrt(t * t * l * l + e * e * i * i), o = Math.abs(t * e * i / c);
  n.x < a && (o = -o);
  var h = Math.abs(t * e * l / c);
  return n.y < s && (h = -h), { x: a + o, y: s + h };
}
function wt(r, t, e) {
  return tt(r, t, t, e);
}
function mt(r, t, e, n) {
  var a, s, i, l, c, o, h, f, b, d, p, x, y, k, H;
  if (a = t.y - r.y, i = r.x - t.x, c = t.x * r.y - r.x * t.y, b = a * e.x + i * e.y + c, d = a * n.x + i * n.y + c, !(b !== 0 && d !== 0 && J(b, d)) && (s = n.y - e.y, l = e.x - n.x, o = n.x * e.y - e.x * n.y, h = s * r.x + l * r.y + o, f = s * t.x + l * t.y + o, !(h !== 0 && f !== 0 && J(h, f)) && (p = a * l - s * i, p !== 0)))
    return x = Math.abs(p / 2), y = i * o - l * c, k = y < 0 ? (y - x) / p : (y + x) / p, y = s * c - a * o, H = y < 0 ? (y - x) / p : (y + x) / p, { x: k, y: H };
}
function J(r, t) {
  return r * t > 0;
}
function kt(r, t, e) {
  var n = r.x, a = r.y, s = [], i = Number.POSITIVE_INFINITY, l = Number.POSITIVE_INFINITY;
  typeof t.forEach == "function" ? t.forEach(function(p) {
    i = Math.min(i, p.x), l = Math.min(l, p.y);
  }) : (i = Math.min(i, t.x), l = Math.min(l, t.y));
  for (var c = n - r.width / 2 - i, o = a - r.height / 2 - l, h = 0; h < t.length; h++) {
    var f = t[h], b = t[h < t.length - 1 ? h + 1 : 0], d = mt(
      r,
      e,
      { x: c + f.x, y: o + f.y },
      { x: c + b.x, y: o + b.y }
    );
    d && s.push(d);
  }
  return s.length ? (s.length > 1 && s.sort(function(p, x) {
    var y = p.x - e.x, k = p.y - e.y, H = Math.sqrt(y * y + k * k), N = x.x - e.x, B = x.y - e.y, X = Math.sqrt(N * N + B * B);
    return H < X ? -1 : H === X ? 0 : 1;
  }), s[0]) : r;
}
const Lt = (r, t) => {
  var e = r.x, n = r.y, a = t.x - e, s = t.y - n, i = r.width / 2, l = r.height / 2, c, o;
  return Math.abs(s) * i > Math.abs(a) * l ? (s < 0 && (l = -l), c = s === 0 ? 0 : l * a / s, o = l) : (a < 0 && (i = -i), c = i, o = a === 0 ? 0 : i * s / a), { x: e + c, y: n + o };
}, vt = Lt, g = {
  node: gt,
  circle: wt,
  ellipse: tt,
  polygon: kt,
  rect: vt
}, St = (r, t) => {
  t.useHtmlLabels || w().flowchart.htmlLabels || (t.centerLabel = !0);
  const { shapeSvg: n, bbox: a, halfPadding: s } = S(r, t, "node " + t.classes, !0);
  u.info("Classes = ", t.classes);
  const i = n.insert("rect", ":first-child");
  return i.attr("rx", t.rx).attr("ry", t.ry).attr("x", -a.width / 2 - s).attr("y", -a.height / 2 - s).attr("width", a.width + t.padding).attr("height", a.height + t.padding), m(t, i), t.intersect = function(l) {
    return g.rect(t, l);
  }, n;
}, Et = St, q = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.width + t.padding, s = n.height + t.padding, i = a + s, l = [
    { x: i / 2, y: 0 },
    { x: i, y: -i / 2 },
    { x: i / 2, y: -i },
    { x: 0, y: -i / 2 }
  ];
  u.info("Question main (Circle)");
  const c = _(e, i, i, l);
  return c.attr("style", t.style), m(t, c), t.intersect = function(o) {
    return u.warn("Intersect called"), g.polygon(t, l, o);
  }, e;
}, Bt = (r, t) => {
  const e = r.insert("g").attr("class", "node default").attr("id", t.domId || t.id), n = 28, a = [
    { x: 0, y: n / 2 },
    { x: n / 2, y: 0 },
    { x: 0, y: -n / 2 },
    { x: -n / 2, y: 0 }
  ];
  return e.insert("polygon", ":first-child").attr(
    "points",
    a.map(function(i) {
      return i.x + "," + i.y;
    }).join(" ")
  ).attr("class", "state-start").attr("r", 7).attr("width", 28).attr("height", 28), t.width = 28, t.height = 28, t.intersect = function(i) {
    return g.circle(t, 14, i);
  }, e;
}, Mt = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = 4, s = n.height + t.padding, i = s / a, l = n.width + 2 * i + t.padding, c = [
    { x: i, y: 0 },
    { x: l - i, y: 0 },
    { x: l, y: -s / 2 },
    { x: l - i, y: -s },
    { x: i, y: -s },
    { x: 0, y: -s / 2 }
  ], o = _(e, l, s, c);
  return o.attr("style", t.style), m(t, o), t.intersect = function(h) {
    return g.polygon(t, c, h);
  }, e;
}, Ct = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.width + t.padding, s = n.height + t.padding, i = [
    { x: -s / 2, y: 0 },
    { x: a, y: 0 },
    { x: a, y: -s },
    { x: -s / 2, y: -s },
    { x: 0, y: -s / 2 }
  ];
  return _(e, a, s, i).attr("style", t.style), t.width = a + s, t.height = s, t.intersect = function(c) {
    return g.polygon(t, i, c);
  }, e;
}, Tt = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.width + t.padding, s = n.height + t.padding, i = [
    { x: -2 * s / 6, y: 0 },
    { x: a - s / 6, y: 0 },
    { x: a + 2 * s / 6, y: -s },
    { x: s / 6, y: -s }
  ], l = _(e, a, s, i);
  return l.attr("style", t.style), m(t, l), t.intersect = function(c) {
    return g.polygon(t, i, c);
  }, e;
}, Rt = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.width + t.padding, s = n.height + t.padding, i = [
    { x: 2 * s / 6, y: 0 },
    { x: a + s / 6, y: 0 },
    { x: a - 2 * s / 6, y: -s },
    { x: -s / 6, y: -s }
  ], l = _(e, a, s, i);
  return l.attr("style", t.style), m(t, l), t.intersect = function(c) {
    return g.polygon(t, i, c);
  }, e;
}, Ht = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.width + t.padding, s = n.height + t.padding, i = [
    { x: -2 * s / 6, y: 0 },
    { x: a + 2 * s / 6, y: 0 },
    { x: a - s / 6, y: -s },
    { x: s / 6, y: -s }
  ], l = _(e, a, s, i);
  return l.attr("style", t.style), m(t, l), t.intersect = function(c) {
    return g.polygon(t, i, c);
  }, e;
}, It = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.width + t.padding, s = n.height + t.padding, i = [
    { x: s / 6, y: 0 },
    { x: a - s / 6, y: 0 },
    { x: a + 2 * s / 6, y: -s },
    { x: -2 * s / 6, y: -s }
  ], l = _(e, a, s, i);
  return l.attr("style", t.style), m(t, l), t.intersect = function(c) {
    return g.polygon(t, i, c);
  }, e;
}, _t = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.width + t.padding, s = n.height + t.padding, i = [
    { x: 0, y: 0 },
    { x: a + s / 2, y: 0 },
    { x: a, y: -s / 2 },
    { x: a + s / 2, y: -s },
    { x: 0, y: -s }
  ], l = _(e, a, s, i);
  return l.attr("style", t.style), m(t, l), t.intersect = function(c) {
    return g.polygon(t, i, c);
  }, e;
}, Nt = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.width + t.padding, s = a / 2, i = s / (2.5 + a / 50), l = n.height + i + t.padding, c = "M 0," + i + " a " + s + "," + i + " 0,0,0 " + a + " 0 a " + s + "," + i + " 0,0,0 " + -a + " 0 l 0," + l + " a " + s + "," + i + " 0,0,0 " + a + " 0 l 0," + -l, o = e.attr("label-offset-y", i).insert("path", ":first-child").attr("style", t.style).attr("d", c).attr("transform", "translate(" + -a / 2 + "," + -(l / 2 + i) + ")");
  return m(t, o), t.intersect = function(h) {
    const f = g.rect(t, h), b = f.x - t.x;
    if (s != 0 && (Math.abs(b) < t.width / 2 || Math.abs(b) == t.width / 2 && Math.abs(f.y - t.y) > t.height / 2 - i)) {
      let d = i * i * (1 - b * b / (s * s));
      d != 0 && (d = Math.sqrt(d)), d = i - d, h.y - t.y > 0 && (d = -d), f.y += d;
    }
    return f;
  }, e;
}, $t = (r, t) => {
  const { shapeSvg: e, bbox: n, halfPadding: a } = S(r, t, "node " + t.classes, !0), s = e.insert("rect", ":first-child"), i = n.width + t.padding, l = n.height + t.padding;
  if (s.attr("class", "basic label-container").attr("style", t.style).attr("rx", t.rx).attr("ry", t.ry).attr("x", -n.width / 2 - a).attr("y", -n.height / 2 - a).attr("width", i).attr("height", l), t.props) {
    const c = new Set(Object.keys(t.props));
    t.props.borders && (rt(s, t.props.borders, i, l), c.delete("borders")), c.forEach((o) => {
      u.warn(`Unknown node property ${o}`);
    });
  }
  return m(t, s), t.intersect = function(c) {
    return g.rect(t, c);
  }, e;
}, Wt = (r, t) => {
  const { shapeSvg: e } = S(r, t, "label", !0);
  u.trace("Classes = ", t.classes);
  const n = e.insert("rect", ":first-child"), a = 0, s = 0;
  if (n.attr("width", a).attr("height", s), e.attr("class", "label edgeLabel"), t.props) {
    const i = new Set(Object.keys(t.props));
    t.props.borders && (rt(n, t.props.borders, a, s), i.delete("borders")), i.forEach((l) => {
      u.warn(`Unknown node property ${l}`);
    });
  }
  return m(t, n), t.intersect = function(i) {
    return g.rect(t, i);
  }, e;
};
function rt(r, t, e, n) {
  const a = [], s = (l) => {
    a.push(l, 0);
  }, i = (l) => {
    a.push(0, l);
  };
  t.includes("t") ? (u.debug("add top border"), s(e)) : i(e), t.includes("r") ? (u.debug("add right border"), s(n)) : i(n), t.includes("b") ? (u.debug("add bottom border"), s(e)) : i(e), t.includes("l") ? (u.debug("add left border"), s(n)) : i(n), r.attr("stroke-dasharray", a.join(" "));
}
const Xt = (r, t) => {
  let e;
  t.classes ? e = "node " + t.classes : e = "node default";
  const n = r.insert("g").attr("class", e).attr("id", t.domId || t.id), a = n.insert("rect", ":first-child"), s = n.insert("line"), i = n.insert("g").attr("class", "label"), l = t.labelText.flat ? t.labelText.flat() : t.labelText;
  let c = "";
  typeof l == "object" ? c = l[0] : c = l, u.info("Label text abc79", c, l, typeof l == "object");
  const o = i.node().appendChild(T(c, t.labelStyle, !0, !0));
  let h = { width: 0, height: 0 };
  if (R(w().flowchart.htmlLabels)) {
    const x = o.children[0], y = v(o);
    h = x.getBoundingClientRect(), y.attr("width", h.width), y.attr("height", h.height);
  }
  u.info("Text 2", l);
  const f = l.slice(1, l.length);
  let b = o.getBBox();
  const d = i.node().appendChild(
    T(f.join ? f.join("<br/>") : f, t.labelStyle, !0, !0)
  );
  if (R(w().flowchart.htmlLabels)) {
    const x = d.children[0], y = v(d);
    h = x.getBoundingClientRect(), y.attr("width", h.width), y.attr("height", h.height);
  }
  const p = t.padding / 2;
  return v(d).attr(
    "transform",
    "translate( " + // (titleBox.width - bbox.width) / 2 +
    (h.width > b.width ? 0 : (b.width - h.width) / 2) + ", " + (b.height + p + 5) + ")"
  ), v(o).attr(
    "transform",
    "translate( " + // (titleBox.width - bbox.width) / 2 +
    (h.width < b.width ? 0 : -(b.width - h.width) / 2) + ", " + 0 + ")"
  ), h = i.node().getBBox(), i.attr(
    "transform",
    "translate(" + -h.width / 2 + ", " + (-h.height / 2 - p + 3) + ")"
  ), a.attr("class", "outer title-state").attr("x", -h.width / 2 - p).attr("y", -h.height / 2 - p).attr("width", h.width + t.padding).attr("height", h.height + t.padding), s.attr("class", "divider").attr("x1", -h.width / 2 - p).attr("x2", h.width / 2 + p).attr("y1", -h.height / 2 - p + b.height + p).attr("y2", -h.height / 2 - p + b.height + p), m(t, a), t.intersect = function(x) {
    return g.rect(t, x);
  }, n;
}, Yt = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.height + t.padding, s = n.width + a / 4 + t.padding, i = e.insert("rect", ":first-child").attr("style", t.style).attr("rx", a / 2).attr("ry", a / 2).attr("x", -s / 2).attr("y", -a / 2).attr("width", s).attr("height", a);
  return m(t, i), t.intersect = function(l) {
    return g.rect(t, l);
  }, e;
}, Ut = (r, t) => {
  const { shapeSvg: e, bbox: n, halfPadding: a } = S(r, t, void 0, !0), s = e.insert("circle", ":first-child");
  return s.attr("style", t.style).attr("rx", t.rx).attr("ry", t.ry).attr("r", n.width / 2 + a).attr("width", n.width + t.padding).attr("height", n.height + t.padding), u.info("Circle main"), m(t, s), t.intersect = function(i) {
    return u.info("Circle intersect", t, n.width / 2 + a, i), g.circle(t, n.width / 2 + a, i);
  }, e;
}, At = (r, t) => {
  const { shapeSvg: e, bbox: n, halfPadding: a } = S(r, t, void 0, !0), s = 5, i = e.insert("g", ":first-child"), l = i.insert("circle"), c = i.insert("circle");
  return l.attr("style", t.style).attr("rx", t.rx).attr("ry", t.ry).attr("r", n.width / 2 + a + s).attr("width", n.width + t.padding + s * 2).attr("height", n.height + t.padding + s * 2), c.attr("style", t.style).attr("rx", t.rx).attr("ry", t.ry).attr("r", n.width / 2 + a).attr("width", n.width + t.padding).attr("height", n.height + t.padding), u.info("DoubleCircle main"), m(t, l), t.intersect = function(o) {
    return u.info("DoubleCircle intersect", t, n.width / 2 + a + s, o), g.circle(t, n.width / 2 + a + s, o);
  }, e;
}, Ot = (r, t) => {
  const { shapeSvg: e, bbox: n } = S(r, t, void 0, !0), a = n.width + t.padding, s = n.height + t.padding, i = [
    { x: 0, y: 0 },
    { x: a, y: 0 },
    { x: a, y: -s },
    { x: 0, y: -s },
    { x: 0, y: 0 },
    { x: -8, y: 0 },
    { x: a + 8, y: 0 },
    { x: a + 8, y: -s },
    { x: -8, y: -s },
    { x: -8, y: 0 }
  ], l = _(e, a, s, i);
  return l.attr("style", t.style), m(t, l), t.intersect = function(c) {
    return g.polygon(t, i, c);
  }, e;
}, jt = (r, t) => {
  const e = r.insert("g").attr("class", "node default").attr("id", t.domId || t.id), n = e.insert("circle", ":first-child");
  return n.attr("class", "state-start").attr("r", 7).attr("width", 14).attr("height", 14), m(t, n), t.intersect = function(a) {
    return g.circle(t, 7, a);
  }, e;
}, F = (r, t, e) => {
  const n = r.insert("g").attr("class", "node default").attr("id", t.domId || t.id);
  let a = 70, s = 10;
  e === "LR" && (a = 10, s = 70);
  const i = n.append("rect").attr("x", -1 * a / 2).attr("y", -1 * s / 2).attr("width", a).attr("height", s).attr("class", "fork-join");
  return m(t, i), t.height = t.height + t.padding / 2, t.width = t.width + t.padding / 2, t.intersect = function(l) {
    return g.rect(t, l);
  }, n;
}, Dt = (r, t) => {
  const e = r.insert("g").attr("class", "node default").attr("id", t.domId || t.id), n = e.insert("circle", ":first-child"), a = e.insert("circle", ":first-child");
  return a.attr("class", "state-start").attr("r", 7).attr("width", 14).attr("height", 14), n.attr("class", "state-end").attr("r", 5).attr("width", 10).attr("height", 10), m(t, a), t.intersect = function(s) {
    return g.circle(t, 7, s);
  }, e;
}, Zt = (r, t) => {
  const e = t.padding / 2, n = 4, a = 8;
  let s;
  t.classes ? s = "node " + t.classes : s = "node default";
  const i = r.insert("g").attr("class", s).attr("id", t.domId || t.id), l = i.insert("rect", ":first-child"), c = i.insert("line"), o = i.insert("line");
  let h = 0, f = n;
  const b = i.insert("g").attr("class", "label");
  let d = 0;
  const p = t.classData.annotations && t.classData.annotations[0], x = t.classData.annotations[0] ? "«" + t.classData.annotations[0] + "»" : "", y = b.node().appendChild(T(x, t.labelStyle, !0, !0));
  let k = y.getBBox();
  if (R(w().flowchart.htmlLabels)) {
    const E = y.children[0], M = v(y);
    k = E.getBoundingClientRect(), M.attr("width", k.width), M.attr("height", k.height);
  }
  t.classData.annotations[0] && (f += k.height + n, h += k.width);
  let H = t.classData.label;
  t.classData.type !== void 0 && t.classData.type !== "" && (w().flowchart.htmlLabels ? H += "&lt;" + t.classData.type + "&gt;" : H += "<" + t.classData.type + ">");
  const N = b.node().appendChild(T(H, t.labelStyle, !0, !0));
  v(N).attr("class", "classTitle");
  let B = N.getBBox();
  if (R(w().flowchart.htmlLabels)) {
    const E = N.children[0], M = v(N);
    B = E.getBoundingClientRect(), M.attr("width", B.width), M.attr("height", B.height);
  }
  f += B.height + n, B.width > h && (h = B.width);
  const X = [];
  t.classData.members.forEach((E) => {
    const M = Q(E);
    let $ = M.displayText;
    w().flowchart.htmlLabels && ($ = $.replace(/</g, "&lt;").replace(/>/g, "&gt;"));
    const I = b.node().appendChild(
      T(
        $,
        M.cssStyle ? M.cssStyle : t.labelStyle,
        !0,
        !0
      )
    );
    let C = I.getBBox();
    if (R(w().flowchart.htmlLabels)) {
      const j = I.children[0], Y = v(I);
      C = j.getBoundingClientRect(), Y.attr("width", C.width), Y.attr("height", C.height);
    }
    C.width > h && (h = C.width), f += C.height + n, X.push(I);
  }), f += a;
  const Z = [];
  if (t.classData.methods.forEach((E) => {
    const M = Q(E);
    let $ = M.displayText;
    w().flowchart.htmlLabels && ($ = $.replace(/</g, "&lt;").replace(/>/g, "&gt;"));
    const I = b.node().appendChild(
      T(
        $,
        M.cssStyle ? M.cssStyle : t.labelStyle,
        !0,
        !0
      )
    );
    let C = I.getBBox();
    if (R(w().flowchart.htmlLabels)) {
      const j = I.children[0], Y = v(I);
      C = j.getBoundingClientRect(), Y.attr("width", C.width), Y.attr("height", C.height);
    }
    C.width > h && (h = C.width), f += C.height + n, Z.push(I);
  }), f += a, p) {
    let E = (h - k.width) / 2;
    v(y).attr(
      "transform",
      "translate( " + (-1 * h / 2 + E) + ", " + -1 * f / 2 + ")"
    ), d = k.height + n;
  }
  let at = (h - B.width) / 2;
  return v(N).attr(
    "transform",
    "translate( " + (-1 * h / 2 + at) + ", " + (-1 * f / 2 + d) + ")"
  ), d += B.height + n, c.attr("class", "divider").attr("x1", -h / 2 - e).attr("x2", h / 2 + e).attr("y1", -f / 2 - e + a + d).attr("y2", -f / 2 - e + a + d), d += a, X.forEach((E) => {
    v(E).attr(
      "transform",
      "translate( " + -h / 2 + ", " + (-1 * f / 2 + d + a / 2) + ")"
    ), d += B.height + n;
  }), d += a, o.attr("class", "divider").attr("x1", -h / 2 - e).attr("x2", h / 2 + e).attr("y1", -f / 2 - e + a + d).attr("y2", -f / 2 - e + a + d), d += a, Z.forEach((E) => {
    v(E).attr(
      "transform",
      "translate( " + -h / 2 + ", " + (-1 * f / 2 + d) + ")"
    ), d += B.height + n;
  }), l.attr("class", "outer title-state").attr("x", -h / 2 - e).attr("y", -(f / 2) - e).attr("width", h + t.padding).attr("height", f + t.padding), m(t, l), t.intersect = function(E) {
    return g.rect(t, E);
  }, i;
}, G = {
  rhombus: q,
  question: q,
  rect: $t,
  labelRect: Wt,
  rectWithTitle: Xt,
  choice: Bt,
  circle: Ut,
  doublecircle: At,
  stadium: Yt,
  hexagon: Mt,
  rect_left_inv_arrow: Ct,
  lean_right: Tt,
  lean_left: Rt,
  trapezoid: Ht,
  inv_trapezoid: It,
  rect_right_inv_arrow: _t,
  cylinder: Nt,
  start: jt,
  end: Dt,
  note: Et,
  subroutine: Ot,
  fork: F,
  join: F,
  class_box: Zt
};
let W = {};
const tr = (r, t, e) => {
  let n, a;
  if (t.link) {
    let s;
    w().securityLevel === "sandbox" ? s = "_top" : t.linkTarget && (s = t.linkTarget || "_blank"), n = r.insert("svg:a").attr("xlink:href", t.link).attr("target", s), a = G[t.shape](n, t, e);
  } else
    a = G[t.shape](r, t, e), n = a;
  return t.tooltip && a.attr("title", t.tooltip), t.class && a.attr("class", "node default " + t.class), W[t.id] = n, t.haveCallback && W[t.id].attr("class", W[t.id].attr("class") + " clickable"), n;
}, rr = (r, t) => {
  W[t.id] = r;
}, ar = () => {
  W = {};
}, er = (r) => {
  const t = W[r.id];
  u.trace(
    "Transforming node",
    r.diff,
    r,
    "translate(" + (r.x - r.width / 2 - 5) + ", " + r.width / 2 + ")"
  );
  const e = 8, n = r.diff || 0;
  return r.clusterNode ? t.attr(
    "transform",
    "translate(" + (r.x + n - r.width / 2) + ", " + (r.y - r.height / 2 - e) + ")"
  ) : t.attr("transform", "translate(" + r.x + ", " + r.y + ")"), n;
};
let O = {}, L = {};
const sr = () => {
  O = {}, L = {};
}, ir = (r, t) => {
  const e = R(w().flowchart.htmlLabels), n = t.labelType === "markdown" ? P(r, t.label, {
    style: t.labelStyle,
    useHtmlLabels: e,
    addSvgBackground: !0
  }) : T(t.label, t.labelStyle);
  u.info("abc82", t, t.labelType);
  const a = r.insert("g").attr("class", "edgeLabel"), s = a.insert("g").attr("class", "label");
  s.node().appendChild(n);
  let i = n.getBBox();
  if (e) {
    const c = n.children[0], o = v(n);
    i = c.getBoundingClientRect(), o.attr("width", i.width), o.attr("height", i.height);
  }
  s.attr("transform", "translate(" + -i.width / 2 + ", " + -i.height / 2 + ")"), O[t.id] = a, t.width = i.width, t.height = i.height;
  let l;
  if (t.startLabelLeft) {
    const c = T(t.startLabelLeft, t.labelStyle), o = r.insert("g").attr("class", "edgeTerminals"), h = o.insert("g").attr("class", "inner");
    l = h.node().appendChild(c);
    const f = c.getBBox();
    h.attr("transform", "translate(" + -f.width / 2 + ", " + -f.height / 2 + ")"), L[t.id] || (L[t.id] = {}), L[t.id].startLeft = o, A(l, t.startLabelLeft);
  }
  if (t.startLabelRight) {
    const c = T(t.startLabelRight, t.labelStyle), o = r.insert("g").attr("class", "edgeTerminals"), h = o.insert("g").attr("class", "inner");
    l = o.node().appendChild(c), h.node().appendChild(c);
    const f = c.getBBox();
    h.attr("transform", "translate(" + -f.width / 2 + ", " + -f.height / 2 + ")"), L[t.id] || (L[t.id] = {}), L[t.id].startRight = o, A(l, t.startLabelRight);
  }
  if (t.endLabelLeft) {
    const c = T(t.endLabelLeft, t.labelStyle), o = r.insert("g").attr("class", "edgeTerminals"), h = o.insert("g").attr("class", "inner");
    l = h.node().appendChild(c);
    const f = c.getBBox();
    h.attr("transform", "translate(" + -f.width / 2 + ", " + -f.height / 2 + ")"), o.node().appendChild(c), L[t.id] || (L[t.id] = {}), L[t.id].endLeft = o, A(l, t.endLabelLeft);
  }
  if (t.endLabelRight) {
    const c = T(t.endLabelRight, t.labelStyle), o = r.insert("g").attr("class", "edgeTerminals"), h = o.insert("g").attr("class", "inner");
    l = h.node().appendChild(c);
    const f = c.getBBox();
    h.attr("transform", "translate(" + -f.width / 2 + ", " + -f.height / 2 + ")"), o.node().appendChild(c), L[t.id] || (L[t.id] = {}), L[t.id].endRight = o, A(l, t.endLabelRight);
  }
  return n;
};
function A(r, t) {
  w().flowchart.htmlLabels && r && (r.style.width = t.length * 9 + "px", r.style.height = "12px");
}
const nr = (r, t) => {
  u.info("Moving label abc78 ", r.id, r.label, O[r.id]);
  let e = t.updatedPath ? t.updatedPath : t.originalPath;
  if (r.label) {
    const n = O[r.id];
    let a = r.x, s = r.y;
    if (e) {
      const i = U.calcLabelPosition(e);
      u.info(
        "Moving label " + r.label + " from (",
        a,
        ",",
        s,
        ") to (",
        i.x,
        ",",
        i.y,
        ") abc78"
      ), t.updatedPath && (a = i.x, s = i.y);
    }
    n.attr("transform", "translate(" + a + ", " + s + ")");
  }
  if (r.startLabelLeft) {
    const n = L[r.id].startLeft;
    let a = r.x, s = r.y;
    if (e) {
      const i = U.calcTerminalLabelPosition(r.arrowTypeStart ? 10 : 0, "start_left", e);
      a = i.x, s = i.y;
    }
    n.attr("transform", "translate(" + a + ", " + s + ")");
  }
  if (r.startLabelRight) {
    const n = L[r.id].startRight;
    let a = r.x, s = r.y;
    if (e) {
      const i = U.calcTerminalLabelPosition(
        r.arrowTypeStart ? 10 : 0,
        "start_right",
        e
      );
      a = i.x, s = i.y;
    }
    n.attr("transform", "translate(" + a + ", " + s + ")");
  }
  if (r.endLabelLeft) {
    const n = L[r.id].endLeft;
    let a = r.x, s = r.y;
    if (e) {
      const i = U.calcTerminalLabelPosition(r.arrowTypeEnd ? 10 : 0, "end_left", e);
      a = i.x, s = i.y;
    }
    n.attr("transform", "translate(" + a + ", " + s + ")");
  }
  if (r.endLabelRight) {
    const n = L[r.id].endRight;
    let a = r.x, s = r.y;
    if (e) {
      const i = U.calcTerminalLabelPosition(r.arrowTypeEnd ? 10 : 0, "end_right", e);
      a = i.x, s = i.y;
    }
    n.attr("transform", "translate(" + a + ", " + s + ")");
  }
}, zt = (r, t) => {
  const e = r.x, n = r.y, a = Math.abs(t.x - e), s = Math.abs(t.y - n), i = r.width / 2, l = r.height / 2;
  return a >= i || s >= l;
}, Qt = (r, t, e) => {
  u.warn(`intersection calc abc89:
  outsidePoint: ${JSON.stringify(t)}
  insidePoint : ${JSON.stringify(e)}
  node        : x:${r.x} y:${r.y} w:${r.width} h:${r.height}`);
  const n = r.x, a = r.y, s = Math.abs(n - e.x), i = r.width / 2;
  let l = e.x < t.x ? i - s : i + s;
  const c = r.height / 2, o = Math.abs(t.y - e.y), h = Math.abs(t.x - e.x);
  if (Math.abs(a - t.y) * i > Math.abs(n - t.x) * c) {
    let f = e.y < t.y ? t.y - c - a : a - c - t.y;
    l = h * f / o;
    const b = {
      x: e.x < t.x ? e.x + l : e.x - h + l,
      y: e.y < t.y ? e.y + o - f : e.y - o + f
    };
    return l === 0 && (b.x = t.x, b.y = t.y), h === 0 && (b.x = t.x), o === 0 && (b.y = t.y), u.warn(`abc89 topp/bott calc, Q ${o}, q ${f}, R ${h}, r ${l}`, b), b;
  } else {
    e.x < t.x ? l = t.x - i - n : l = n - i - t.x;
    let f = o * l / h, b = e.x < t.x ? e.x + h - l : e.x - h + l, d = e.y < t.y ? e.y + f : e.y - f;
    return u.warn(`sides calc abc89, Q ${o}, q ${f}, R ${h}, r ${l}`, { _x: b, _y: d }), l === 0 && (b = t.x, d = t.y), h === 0 && (b = t.x), o === 0 && (d = t.y), { x: b, y: d };
  }
}, K = (r, t) => {
  u.warn("abc88 cutPathAtIntersect", r, t);
  let e = [], n = r[0], a = !1;
  return r.forEach((s) => {
    if (u.info("abc88 checking point", s, t), !zt(t, s) && !a) {
      const i = Qt(t, n, s);
      u.warn("abc88 inside", s, n, i), u.warn("abc88 intersection", i);
      let l = !1;
      e.forEach((c) => {
        l = l || c.x === i.x && c.y === i.y;
      }), e.some((c) => c.x === i.x && c.y === i.y) ? u.warn("abc88 no intersect", i, e) : e.push(i), a = !0;
    } else
      u.warn("abc88 outside", s, n), n = s, a || e.push(s);
  }), u.warn("abc88 returning points", e), e;
}, lr = function(r, t, e, n, a, s) {
  let i = e.points, l = !1;
  const c = s.node(t.v);
  var o = s.node(t.w);
  u.info("abc88 InsertEdge: ", e), o.intersect && c.intersect && (i = i.slice(1, e.points.length - 1), i.unshift(c.intersect(i[0])), u.info(
    "Last point",
    i[i.length - 1],
    o,
    o.intersect(i[i.length - 1])
  ), i.push(o.intersect(i[i.length - 1]))), e.toCluster && (u.info("to cluster abc88", n[e.toCluster]), i = K(e.points, n[e.toCluster].node), l = !0), e.fromCluster && (u.info("from cluster abc88", n[e.fromCluster]), i = K(i.reverse(), n[e.fromCluster].node).reverse(), l = !0);
  const h = i.filter((k) => !Number.isNaN(k.y));
  let f;
  a === "graph" || a === "flowchart" ? f = e.curve || V : f = V;
  const b = et().x(function(k) {
    return k.x;
  }).y(function(k) {
    return k.y;
  }).curve(f);
  let d;
  switch (e.thickness) {
    case "normal":
      d = "edge-thickness-normal";
      break;
    case "thick":
      d = "edge-thickness-thick";
      break;
    case "invisible":
      d = "edge-thickness-thick";
      break;
    default:
      d = "";
  }
  switch (e.pattern) {
    case "solid":
      d += " edge-pattern-solid";
      break;
    case "dotted":
      d += " edge-pattern-dotted";
      break;
    case "dashed":
      d += " edge-pattern-dashed";
      break;
  }
  const p = r.append("path").attr("d", b(h)).attr("id", e.id).attr("class", " " + d + (e.classes ? " " + e.classes : "")).attr("style", e.style);
  let x = "";
  switch ((w().flowchart.arrowMarkerAbsolute || w().state.arrowMarkerAbsolute) && (x = window.location.protocol + "//" + window.location.host + window.location.pathname + window.location.search, x = x.replace(/\(/g, "\\("), x = x.replace(/\)/g, "\\)")), u.info("arrowTypeStart", e.arrowTypeStart), u.info("arrowTypeEnd", e.arrowTypeEnd), e.arrowTypeStart) {
    case "arrow_cross":
      p.attr("marker-start", "url(" + x + "#" + a + "-crossStart)");
      break;
    case "arrow_point":
      p.attr("marker-start", "url(" + x + "#" + a + "-pointStart)");
      break;
    case "arrow_barb":
      p.attr("marker-start", "url(" + x + "#" + a + "-barbStart)");
      break;
    case "arrow_circle":
      p.attr("marker-start", "url(" + x + "#" + a + "-circleStart)");
      break;
    case "aggregation":
      p.attr("marker-start", "url(" + x + "#" + a + "-aggregationStart)");
      break;
    case "extension":
      p.attr("marker-start", "url(" + x + "#" + a + "-extensionStart)");
      break;
    case "composition":
      p.attr("marker-start", "url(" + x + "#" + a + "-compositionStart)");
      break;
    case "dependency":
      p.attr("marker-start", "url(" + x + "#" + a + "-dependencyStart)");
      break;
    case "lollipop":
      p.attr("marker-start", "url(" + x + "#" + a + "-lollipopStart)");
      break;
  }
  switch (e.arrowTypeEnd) {
    case "arrow_cross":
      p.attr("marker-end", "url(" + x + "#" + a + "-crossEnd)");
      break;
    case "arrow_point":
      p.attr("marker-end", "url(" + x + "#" + a + "-pointEnd)");
      break;
    case "arrow_barb":
      p.attr("marker-end", "url(" + x + "#" + a + "-barbEnd)");
      break;
    case "arrow_circle":
      p.attr("marker-end", "url(" + x + "#" + a + "-circleEnd)");
      break;
    case "aggregation":
      p.attr("marker-end", "url(" + x + "#" + a + "-aggregationEnd)");
      break;
    case "extension":
      p.attr("marker-end", "url(" + x + "#" + a + "-extensionEnd)");
      break;
    case "composition":
      p.attr("marker-end", "url(" + x + "#" + a + "-compositionEnd)");
      break;
    case "dependency":
      p.attr("marker-end", "url(" + x + "#" + a + "-dependencyEnd)");
      break;
    case "lollipop":
      p.attr("marker-end", "url(" + x + "#" + a + "-lollipopEnd)");
      break;
  }
  let y = {};
  return l && (y.updatedPath = i), y.originalPath = e.points, y;
};
export {
  Pt as a,
  ar as b,
  T as c,
  sr as d,
  tr as e,
  ir as f,
  lr as g,
  nr as h,
  vt as i,
  S as l,
  er as p,
  rr as s,
  m as u
};
//# sourceMappingURL=edges-de377bae.js.map
