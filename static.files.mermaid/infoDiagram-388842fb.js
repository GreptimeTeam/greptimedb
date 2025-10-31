import { A as C, l as E, g as D, h as P } from "./commonDb-41f8b4c5.js";
var L = function() {
  var h = function(c, t, e, n) {
    for (e = e || {}, n = c.length; n--; e[c[n]] = t)
      ;
    return e;
  }, f = [6, 9, 10], x = {
    trace: function() {
    },
    yy: {},
    symbols_: { error: 2, start: 3, info: 4, document: 5, EOF: 6, line: 7, statement: 8, NL: 9, showInfo: 10, $accept: 0, $end: 1 },
    terminals_: { 2: "error", 4: "info", 6: "EOF", 9: "NL", 10: "showInfo" },
    productions_: [0, [3, 3], [5, 0], [5, 2], [7, 1], [7, 1], [8, 1]],
    performAction: function(t, e, n, s, r, i, k) {
      switch (i.length - 1, r) {
        case 1:
          return s;
        case 4:
          break;
        case 6:
          s.setInfo(!0);
          break;
      }
    },
    table: [{ 3: 1, 4: [1, 2] }, { 1: [3] }, h(f, [2, 2], { 5: 3 }), { 6: [1, 4], 7: 5, 8: 6, 9: [1, 7], 10: [1, 8] }, { 1: [2, 1] }, h(f, [2, 3]), h(f, [2, 4]), h(f, [2, 5]), h(f, [2, 6])],
    defaultActions: { 4: [2, 1] },
    parseError: function(t, e) {
      if (e.recoverable)
        this.trace(t);
      else {
        var n = new Error(t);
        throw n.hash = e, n;
      }
    },
    parse: function(t) {
      var e = this, n = [0], s = [], r = [null], i = [], k = this.table, $ = "", b = 0, T = 0, z = 2, R = 1, M = i.slice.call(arguments, 1), o = Object.create(this.lexer), p = { yy: {} };
      for (var w in this.yy)
        Object.prototype.hasOwnProperty.call(this.yy, w) && (p.yy[w] = this.yy[w]);
      o.setInput(t, p.yy), p.yy.lexer = o, p.yy.parser = this, typeof o.yylloc > "u" && (o.yylloc = {});
      var I = o.yylloc;
      i.push(I);
      var Y = o.options && o.options.ranges;
      typeof p.yy.parseError == "function" ? this.parseError = p.yy.parseError : this.parseError = Object.getPrototypeOf(this).parseError;
      function U() {
        var u;
        return u = s.pop() || o.lex() || R, typeof u != "number" && (u instanceof Array && (s = u, u = s.pop()), u = e.symbols_[u] || u), u;
      }
      for (var l, g, a, A, d = {}, v, y, j, S; ; ) {
        if (g = n[n.length - 1], this.defaultActions[g] ? a = this.defaultActions[g] : ((l === null || typeof l > "u") && (l = U()), a = k[g] && k[g][l]), typeof a > "u" || !a.length || !a[0]) {
          var O = "";
          S = [];
          for (v in k[g])
            this.terminals_[v] && v > z && S.push("'" + this.terminals_[v] + "'");
          o.showPosition ? O = "Parse error on line " + (b + 1) + `:
` + o.showPosition() + `
Expecting ` + S.join(", ") + ", got '" + (this.terminals_[l] || l) + "'" : O = "Parse error on line " + (b + 1) + ": Unexpected " + (l == R ? "end of input" : "'" + (this.terminals_[l] || l) + "'"), this.parseError(O, {
            text: o.match,
            token: this.terminals_[l] || l,
            line: o.yylineno,
            loc: I,
            expected: S
          });
        }
        if (a[0] instanceof Array && a.length > 1)
          throw new Error("Parse Error: multiple actions possible at state: " + g + ", token: " + l);
        switch (a[0]) {
          case 1:
            n.push(l), r.push(o.yytext), i.push(o.yylloc), n.push(a[1]), l = null, T = o.yyleng, $ = o.yytext, b = o.yylineno, I = o.yylloc;
            break;
          case 2:
            if (y = this.productions_[a[1]][1], d.$ = r[r.length - y], d._$ = {
              first_line: i[i.length - (y || 1)].first_line,
              last_line: i[i.length - 1].last_line,
              first_column: i[i.length - (y || 1)].first_column,
              last_column: i[i.length - 1].last_column
            }, Y && (d._$.range = [
              i[i.length - (y || 1)].range[0],
              i[i.length - 1].range[1]
            ]), A = this.performAction.apply(d, [
              $,
              T,
              b,
              p.yy,
              a[1],
              r,
              i
            ].concat(M)), typeof A < "u")
              return A;
            y && (n = n.slice(0, -1 * y * 2), r = r.slice(0, -1 * y), i = i.slice(0, -1 * y)), n.push(this.productions_[a[1]][0]), r.push(d.$), i.push(d._$), j = k[n[n.length - 2]][n[n.length - 1]], n.push(j);
            break;
          case 3:
            return !0;
        }
      }
      return !0;
    }
  }, m = function() {
    var c = {
      EOF: 1,
      parseError: function(e, n) {
        if (this.yy.parser)
          this.yy.parser.parseError(e, n);
        else
          throw new Error(e);
      },
      // resets the lexer, sets new input
      setInput: function(t, e) {
        return this.yy = e || this.yy || {}, this._input = t, this._more = this._backtrack = this.done = !1, this.yylineno = this.yyleng = 0, this.yytext = this.matched = this.match = "", this.conditionStack = ["INITIAL"], this.yylloc = {
          first_line: 1,
          first_column: 0,
          last_line: 1,
          last_column: 0
        }, this.options.ranges && (this.yylloc.range = [0, 0]), this.offset = 0, this;
      },
      // consumes and returns one char from the input
      input: function() {
        var t = this._input[0];
        this.yytext += t, this.yyleng++, this.offset++, this.match += t, this.matched += t;
        var e = t.match(/(?:\r\n?|\n).*/g);
        return e ? (this.yylineno++, this.yylloc.last_line++) : this.yylloc.last_column++, this.options.ranges && this.yylloc.range[1]++, this._input = this._input.slice(1), t;
      },
      // unshifts one char (or a string) into the input
      unput: function(t) {
        var e = t.length, n = t.split(/(?:\r\n?|\n)/g);
        this._input = t + this._input, this.yytext = this.yytext.substr(0, this.yytext.length - e), this.offset -= e;
        var s = this.match.split(/(?:\r\n?|\n)/g);
        this.match = this.match.substr(0, this.match.length - 1), this.matched = this.matched.substr(0, this.matched.length - 1), n.length - 1 && (this.yylineno -= n.length - 1);
        var r = this.yylloc.range;
        return this.yylloc = {
          first_line: this.yylloc.first_line,
          last_line: this.yylineno + 1,
          first_column: this.yylloc.first_column,
          last_column: n ? (n.length === s.length ? this.yylloc.first_column : 0) + s[s.length - n.length].length - n[0].length : this.yylloc.first_column - e
        }, this.options.ranges && (this.yylloc.range = [r[0], r[0] + this.yyleng - e]), this.yyleng = this.yytext.length, this;
      },
      // When called from action, caches matched text and appends it on next action
      more: function() {
        return this._more = !0, this;
      },
      // When called from action, signals the lexer that this rule fails to match the input, so the next matching rule (regex) should be tested instead.
      reject: function() {
        if (this.options.backtrack_lexer)
          this._backtrack = !0;
        else
          return this.parseError("Lexical error on line " + (this.yylineno + 1) + `. You can only invoke reject() in the lexer when the lexer is of the backtracking persuasion (options.backtrack_lexer = true).
` + this.showPosition(), {
            text: "",
            token: null,
            line: this.yylineno
          });
        return this;
      },
      // retain first n characters of the match
      less: function(t) {
        this.unput(this.match.slice(t));
      },
      // displays already matched input, i.e. for error messages
      pastInput: function() {
        var t = this.matched.substr(0, this.matched.length - this.match.length);
        return (t.length > 20 ? "..." : "") + t.substr(-20).replace(/\n/g, "");
      },
      // displays upcoming input, i.e. for error messages
      upcomingInput: function() {
        var t = this.match;
        return t.length < 20 && (t += this._input.substr(0, 20 - t.length)), (t.substr(0, 20) + (t.length > 20 ? "..." : "")).replace(/\n/g, "");
      },
      // displays the character position where the lexing error occurred, i.e. for error messages
      showPosition: function() {
        var t = this.pastInput(), e = new Array(t.length + 1).join("-");
        return t + this.upcomingInput() + `
` + e + "^";
      },
      // test the lexed token: return FALSE when not a match, otherwise return token
      test_match: function(t, e) {
        var n, s, r;
        if (this.options.backtrack_lexer && (r = {
          yylineno: this.yylineno,
          yylloc: {
            first_line: this.yylloc.first_line,
            last_line: this.last_line,
            first_column: this.yylloc.first_column,
            last_column: this.yylloc.last_column
          },
          yytext: this.yytext,
          match: this.match,
          matches: this.matches,
          matched: this.matched,
          yyleng: this.yyleng,
          offset: this.offset,
          _more: this._more,
          _input: this._input,
          yy: this.yy,
          conditionStack: this.conditionStack.slice(0),
          done: this.done
        }, this.options.ranges && (r.yylloc.range = this.yylloc.range.slice(0))), s = t[0].match(/(?:\r\n?|\n).*/g), s && (this.yylineno += s.length), this.yylloc = {
          first_line: this.yylloc.last_line,
          last_line: this.yylineno + 1,
          first_column: this.yylloc.last_column,
          last_column: s ? s[s.length - 1].length - s[s.length - 1].match(/\r?\n?/)[0].length : this.yylloc.last_column + t[0].length
        }, this.yytext += t[0], this.match += t[0], this.matches = t, this.yyleng = this.yytext.length, this.options.ranges && (this.yylloc.range = [this.offset, this.offset += this.yyleng]), this._more = !1, this._backtrack = !1, this._input = this._input.slice(t[0].length), this.matched += t[0], n = this.performAction.call(this, this.yy, this, e, this.conditionStack[this.conditionStack.length - 1]), this.done && this._input && (this.done = !1), n)
          return n;
        if (this._backtrack) {
          for (var i in r)
            this[i] = r[i];
          return !1;
        }
        return !1;
      },
      // return next match in input
      next: function() {
        if (this.done)
          return this.EOF;
        this._input || (this.done = !0);
        var t, e, n, s;
        this._more || (this.yytext = "", this.match = "");
        for (var r = this._currentRules(), i = 0; i < r.length; i++)
          if (n = this._input.match(this.rules[r[i]]), n && (!e || n[0].length > e[0].length)) {
            if (e = n, s = i, this.options.backtrack_lexer) {
              if (t = this.test_match(n, r[i]), t !== !1)
                return t;
              if (this._backtrack) {
                e = !1;
                continue;
              } else
                return !1;
            } else if (!this.options.flex)
              break;
          }
        return e ? (t = this.test_match(e, r[s]), t !== !1 ? t : !1) : this._input === "" ? this.EOF : this.parseError("Lexical error on line " + (this.yylineno + 1) + `. Unrecognized text.
` + this.showPosition(), {
          text: "",
          token: null,
          line: this.yylineno
        });
      },
      // return next match that has a token
      lex: function() {
        var e = this.next();
        return e || this.lex();
      },
      // activates a new lexer condition state (pushes the new lexer condition state onto the condition stack)
      begin: function(e) {
        this.conditionStack.push(e);
      },
      // pop the previously active lexer condition state off the condition stack
      popState: function() {
        var e = this.conditionStack.length - 1;
        return e > 0 ? this.conditionStack.pop() : this.conditionStack[0];
      },
      // produce the lexer rule set which is active for the currently active lexer condition state
      _currentRules: function() {
        return this.conditionStack.length && this.conditionStack[this.conditionStack.length - 1] ? this.conditions[this.conditionStack[this.conditionStack.length - 1]].rules : this.conditions.INITIAL.rules;
      },
      // return the currently active lexer condition state; when an index argument is provided it produces the N-th previous condition state, if available
      topState: function(e) {
        return e = this.conditionStack.length - 1 - Math.abs(e || 0), e >= 0 ? this.conditionStack[e] : "INITIAL";
      },
      // alias for begin(condition)
      pushState: function(e) {
        this.begin(e);
      },
      // return the number of states currently on the stack
      stateStackSize: function() {
        return this.conditionStack.length;
      },
      options: { "case-insensitive": !0 },
      performAction: function(e, n, s, r) {
        switch (s) {
          case 0:
            return 4;
          case 1:
            return 9;
          case 2:
            return "space";
          case 3:
            return 10;
          case 4:
            return 6;
          case 5:
            return "TXT";
        }
      },
      rules: [/^(?:info\b)/i, /^(?:[\s\n\r]+)/i, /^(?:[\s]+)/i, /^(?:showInfo\b)/i, /^(?:$)/i, /^(?:.)/i],
      conditions: { INITIAL: { rules: [0, 1, 2, 3, 4, 5], inclusive: !0 } }
    };
    return c;
  }();
  x.lexer = m;
  function _() {
    this.yy = {};
  }
  return _.prototype = x, x.Parser = _, new _();
}();
L.parser = L;
const V = L;
var F = "", N = !1;
const X = (h) => {
  E.debug("Setting message to: " + h), F = h;
}, q = () => F, B = (h) => {
  N = h;
}, G = () => N, H = {
  setMessage: X,
  getMessage: q,
  setInfo: B,
  getInfo: G,
  clear: C
  // parseError
}, J = () => "", K = J, Q = (h, f, x) => {
  try {
    E.debug(`Rendering info diagram
` + h);
    const m = D().securityLevel;
    let _;
    m === "sandbox" && (_ = P("#i" + f));
    const t = (m === "sandbox" ? P(_.nodes()[0].contentDocument.body) : P("body")).select("#" + f);
    t.append("g").append("text").attr("x", 100).attr("y", 40).attr("class", "version").attr("font-size", "32px").style("text-anchor", "middle").text("v " + x), t.attr("height", 100), t.attr("width", 400);
  } catch (m) {
    E.error("Error while rendering info diagram"), E.error(m.message);
  }
}, W = {
  draw: Q
}, tt = {
  parser: V,
  db: H,
  renderer: W,
  styles: K
};
export {
  tt as diagram
};
//# sourceMappingURL=infoDiagram-388842fb.js.map
