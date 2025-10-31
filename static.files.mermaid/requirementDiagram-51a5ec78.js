import { g as xe, q as Fe, v as Me, x as Pe, w as Ye, l as ve, A as Ue, h as oe, f as Te } from "./commonDb-41f8b4c5.js";
import { m as Be } from "./mermaidAPI-67f627de.js";
import { G as Qe, l as He } from "./layout-492ec81d.js";
import { v as We } from "./utils-8ea37061.js";
import { l as Ke } from "./line-05ccbb85.js";
import "./array-2ff2c7a6.js";
import "./constant-2fe7eae5.js";
var he = function() {
  var e = function(q, r, s, l) {
    for (s = s || {}, l = q.length; l--; s[q[l]] = r)
      ;
    return s;
  }, t = [1, 3], a = [1, 5], c = [1, 6], d = [1, 7], u = [1, 8], p = [5, 6, 8, 14, 16, 18, 19, 40, 41, 42, 43, 44, 45, 53, 71, 72], h = [1, 22], o = [2, 13], g = [1, 26], R = [1, 27], x = [1, 28], S = [1, 29], T = [1, 30], v = [1, 31], A = [1, 24], N = [1, 32], w = [1, 33], pe = [1, 36], F = [71, 72], fe = [5, 8, 14, 16, 18, 19, 40, 41, 42, 43, 44, 45, 53, 60, 62, 71, 72], _e = [1, 56], ye = [1, 57], ge = [1, 58], Ee = [1, 59], Re = [1, 60], me = [1, 61], Ie = [1, 62], O = [62, 63], M = [1, 74], P = [1, 70], Y = [1, 71], U = [1, 72], B = [1, 73], Q = [1, 75], j = [1, 79], X = [1, 80], J = [1, 77], Z = [1, 78], m = [5, 8, 14, 16, 18, 19, 40, 41, 42, 43, 44, 45, 53, 71, 72], re = {
    trace: function() {
    },
    yy: {},
    symbols_: { error: 2, start: 3, directive: 4, NEWLINE: 5, RD: 6, diagram: 7, EOF: 8, openDirective: 9, typeDirective: 10, closeDirective: 11, ":": 12, argDirective: 13, acc_title: 14, acc_title_value: 15, acc_descr: 16, acc_descr_value: 17, acc_descr_multiline_value: 18, open_directive: 19, type_directive: 20, arg_directive: 21, close_directive: 22, requirementDef: 23, elementDef: 24, relationshipDef: 25, requirementType: 26, requirementName: 27, STRUCT_START: 28, requirementBody: 29, ID: 30, COLONSEP: 31, id: 32, TEXT: 33, text: 34, RISK: 35, riskLevel: 36, VERIFYMTHD: 37, verifyType: 38, STRUCT_STOP: 39, REQUIREMENT: 40, FUNCTIONAL_REQUIREMENT: 41, INTERFACE_REQUIREMENT: 42, PERFORMANCE_REQUIREMENT: 43, PHYSICAL_REQUIREMENT: 44, DESIGN_CONSTRAINT: 45, LOW_RISK: 46, MED_RISK: 47, HIGH_RISK: 48, VERIFY_ANALYSIS: 49, VERIFY_DEMONSTRATION: 50, VERIFY_INSPECTION: 51, VERIFY_TEST: 52, ELEMENT: 53, elementName: 54, elementBody: 55, TYPE: 56, type: 57, DOCREF: 58, ref: 59, END_ARROW_L: 60, relationship: 61, LINE: 62, END_ARROW_R: 63, CONTAINS: 64, COPIES: 65, DERIVES: 66, SATISFIES: 67, VERIFIES: 68, REFINES: 69, TRACES: 70, unqString: 71, qString: 72, $accept: 0, $end: 1 },
    terminals_: { 2: "error", 5: "NEWLINE", 6: "RD", 8: "EOF", 12: ":", 14: "acc_title", 15: "acc_title_value", 16: "acc_descr", 17: "acc_descr_value", 18: "acc_descr_multiline_value", 19: "open_directive", 20: "type_directive", 21: "arg_directive", 22: "close_directive", 28: "STRUCT_START", 30: "ID", 31: "COLONSEP", 33: "TEXT", 35: "RISK", 37: "VERIFYMTHD", 39: "STRUCT_STOP", 40: "REQUIREMENT", 41: "FUNCTIONAL_REQUIREMENT", 42: "INTERFACE_REQUIREMENT", 43: "PERFORMANCE_REQUIREMENT", 44: "PHYSICAL_REQUIREMENT", 45: "DESIGN_CONSTRAINT", 46: "LOW_RISK", 47: "MED_RISK", 48: "HIGH_RISK", 49: "VERIFY_ANALYSIS", 50: "VERIFY_DEMONSTRATION", 51: "VERIFY_INSPECTION", 52: "VERIFY_TEST", 53: "ELEMENT", 56: "TYPE", 58: "DOCREF", 60: "END_ARROW_L", 62: "LINE", 63: "END_ARROW_R", 64: "CONTAINS", 65: "COPIES", 66: "DERIVES", 67: "SATISFIES", 68: "VERIFIES", 69: "REFINES", 70: "TRACES", 71: "unqString", 72: "qString" },
    productions_: [0, [3, 3], [3, 2], [3, 4], [4, 3], [4, 5], [4, 2], [4, 2], [4, 1], [9, 1], [10, 1], [13, 1], [11, 1], [7, 0], [7, 2], [7, 2], [7, 2], [7, 2], [7, 2], [23, 5], [29, 5], [29, 5], [29, 5], [29, 5], [29, 2], [29, 1], [26, 1], [26, 1], [26, 1], [26, 1], [26, 1], [26, 1], [36, 1], [36, 1], [36, 1], [38, 1], [38, 1], [38, 1], [38, 1], [24, 5], [55, 5], [55, 5], [55, 2], [55, 1], [25, 5], [25, 5], [61, 1], [61, 1], [61, 1], [61, 1], [61, 1], [61, 1], [61, 1], [27, 1], [27, 1], [32, 1], [32, 1], [34, 1], [34, 1], [54, 1], [54, 1], [57, 1], [57, 1], [59, 1], [59, 1]],
    performAction: function(r, s, l, i, _, n, K) {
      var f = n.length - 1;
      switch (_) {
        case 6:
          this.$ = n[f].trim(), i.setAccTitle(this.$);
          break;
        case 7:
        case 8:
          this.$ = n[f].trim(), i.setAccDescription(this.$);
          break;
        case 9:
          i.parseDirective("%%{", "open_directive");
          break;
        case 10:
          i.parseDirective(n[f], "type_directive");
          break;
        case 11:
          n[f] = n[f].trim().replace(/'/g, '"'), i.parseDirective(n[f], "arg_directive");
          break;
        case 12:
          i.parseDirective("}%%", "close_directive", "pie");
          break;
        case 13:
          this.$ = [];
          break;
        case 19:
          i.addRequirement(n[f - 3], n[f - 4]);
          break;
        case 20:
          i.setNewReqId(n[f - 2]);
          break;
        case 21:
          i.setNewReqText(n[f - 2]);
          break;
        case 22:
          i.setNewReqRisk(n[f - 2]);
          break;
        case 23:
          i.setNewReqVerifyMethod(n[f - 2]);
          break;
        case 26:
          this.$ = i.RequirementType.REQUIREMENT;
          break;
        case 27:
          this.$ = i.RequirementType.FUNCTIONAL_REQUIREMENT;
          break;
        case 28:
          this.$ = i.RequirementType.INTERFACE_REQUIREMENT;
          break;
        case 29:
          this.$ = i.RequirementType.PERFORMANCE_REQUIREMENT;
          break;
        case 30:
          this.$ = i.RequirementType.PHYSICAL_REQUIREMENT;
          break;
        case 31:
          this.$ = i.RequirementType.DESIGN_CONSTRAINT;
          break;
        case 32:
          this.$ = i.RiskLevel.LOW_RISK;
          break;
        case 33:
          this.$ = i.RiskLevel.MED_RISK;
          break;
        case 34:
          this.$ = i.RiskLevel.HIGH_RISK;
          break;
        case 35:
          this.$ = i.VerifyType.VERIFY_ANALYSIS;
          break;
        case 36:
          this.$ = i.VerifyType.VERIFY_DEMONSTRATION;
          break;
        case 37:
          this.$ = i.VerifyType.VERIFY_INSPECTION;
          break;
        case 38:
          this.$ = i.VerifyType.VERIFY_TEST;
          break;
        case 39:
          i.addElement(n[f - 3]);
          break;
        case 40:
          i.setNewElementType(n[f - 2]);
          break;
        case 41:
          i.setNewElementDocRef(n[f - 2]);
          break;
        case 44:
          i.addRelationship(n[f - 2], n[f], n[f - 4]);
          break;
        case 45:
          i.addRelationship(n[f - 2], n[f - 4], n[f]);
          break;
        case 46:
          this.$ = i.Relationships.CONTAINS;
          break;
        case 47:
          this.$ = i.Relationships.COPIES;
          break;
        case 48:
          this.$ = i.Relationships.DERIVES;
          break;
        case 49:
          this.$ = i.Relationships.SATISFIES;
          break;
        case 50:
          this.$ = i.Relationships.VERIFIES;
          break;
        case 51:
          this.$ = i.Relationships.REFINES;
          break;
        case 52:
          this.$ = i.Relationships.TRACES;
          break;
      }
    },
    table: [{ 3: 1, 4: 2, 6: t, 9: 4, 14: a, 16: c, 18: d, 19: u }, { 1: [3] }, { 3: 10, 4: 2, 5: [1, 9], 6: t, 9: 4, 14: a, 16: c, 18: d, 19: u }, { 5: [1, 11] }, { 10: 12, 20: [1, 13] }, { 15: [1, 14] }, { 17: [1, 15] }, e(p, [2, 8]), { 20: [2, 9] }, { 3: 16, 4: 2, 6: t, 9: 4, 14: a, 16: c, 18: d, 19: u }, { 1: [2, 2] }, { 4: 21, 5: h, 7: 17, 8: o, 9: 4, 14: a, 16: c, 18: d, 19: u, 23: 18, 24: 19, 25: 20, 26: 23, 32: 25, 40: g, 41: R, 42: x, 43: S, 44: T, 45: v, 53: A, 71: N, 72: w }, { 11: 34, 12: [1, 35], 22: pe }, e([12, 22], [2, 10]), e(p, [2, 6]), e(p, [2, 7]), { 1: [2, 1] }, { 8: [1, 37] }, { 4: 21, 5: h, 7: 38, 8: o, 9: 4, 14: a, 16: c, 18: d, 19: u, 23: 18, 24: 19, 25: 20, 26: 23, 32: 25, 40: g, 41: R, 42: x, 43: S, 44: T, 45: v, 53: A, 71: N, 72: w }, { 4: 21, 5: h, 7: 39, 8: o, 9: 4, 14: a, 16: c, 18: d, 19: u, 23: 18, 24: 19, 25: 20, 26: 23, 32: 25, 40: g, 41: R, 42: x, 43: S, 44: T, 45: v, 53: A, 71: N, 72: w }, { 4: 21, 5: h, 7: 40, 8: o, 9: 4, 14: a, 16: c, 18: d, 19: u, 23: 18, 24: 19, 25: 20, 26: 23, 32: 25, 40: g, 41: R, 42: x, 43: S, 44: T, 45: v, 53: A, 71: N, 72: w }, { 4: 21, 5: h, 7: 41, 8: o, 9: 4, 14: a, 16: c, 18: d, 19: u, 23: 18, 24: 19, 25: 20, 26: 23, 32: 25, 40: g, 41: R, 42: x, 43: S, 44: T, 45: v, 53: A, 71: N, 72: w }, { 4: 21, 5: h, 7: 42, 8: o, 9: 4, 14: a, 16: c, 18: d, 19: u, 23: 18, 24: 19, 25: 20, 26: 23, 32: 25, 40: g, 41: R, 42: x, 43: S, 44: T, 45: v, 53: A, 71: N, 72: w }, { 27: 43, 71: [1, 44], 72: [1, 45] }, { 54: 46, 71: [1, 47], 72: [1, 48] }, { 60: [1, 49], 62: [1, 50] }, e(F, [2, 26]), e(F, [2, 27]), e(F, [2, 28]), e(F, [2, 29]), e(F, [2, 30]), e(F, [2, 31]), e(fe, [2, 55]), e(fe, [2, 56]), e(p, [2, 4]), { 13: 51, 21: [1, 52] }, e(p, [2, 12]), { 1: [2, 3] }, { 8: [2, 14] }, { 8: [2, 15] }, { 8: [2, 16] }, { 8: [2, 17] }, { 8: [2, 18] }, { 28: [1, 53] }, { 28: [2, 53] }, { 28: [2, 54] }, { 28: [1, 54] }, { 28: [2, 59] }, { 28: [2, 60] }, { 61: 55, 64: _e, 65: ye, 66: ge, 67: Ee, 68: Re, 69: me, 70: Ie }, { 61: 63, 64: _e, 65: ye, 66: ge, 67: Ee, 68: Re, 69: me, 70: Ie }, { 11: 64, 22: pe }, { 22: [2, 11] }, { 5: [1, 65] }, { 5: [1, 66] }, { 62: [1, 67] }, e(O, [2, 46]), e(O, [2, 47]), e(O, [2, 48]), e(O, [2, 49]), e(O, [2, 50]), e(O, [2, 51]), e(O, [2, 52]), { 63: [1, 68] }, e(p, [2, 5]), { 5: M, 29: 69, 30: P, 33: Y, 35: U, 37: B, 39: Q }, { 5: j, 39: X, 55: 76, 56: J, 58: Z }, { 32: 81, 71: N, 72: w }, { 32: 82, 71: N, 72: w }, e(m, [2, 19]), { 31: [1, 83] }, { 31: [1, 84] }, { 31: [1, 85] }, { 31: [1, 86] }, { 5: M, 29: 87, 30: P, 33: Y, 35: U, 37: B, 39: Q }, e(m, [2, 25]), e(m, [2, 39]), { 31: [1, 88] }, { 31: [1, 89] }, { 5: j, 39: X, 55: 90, 56: J, 58: Z }, e(m, [2, 43]), e(m, [2, 44]), e(m, [2, 45]), { 32: 91, 71: N, 72: w }, { 34: 92, 71: [1, 93], 72: [1, 94] }, { 36: 95, 46: [1, 96], 47: [1, 97], 48: [1, 98] }, { 38: 99, 49: [1, 100], 50: [1, 101], 51: [1, 102], 52: [1, 103] }, e(m, [2, 24]), { 57: 104, 71: [1, 105], 72: [1, 106] }, { 59: 107, 71: [1, 108], 72: [1, 109] }, e(m, [2, 42]), { 5: [1, 110] }, { 5: [1, 111] }, { 5: [2, 57] }, { 5: [2, 58] }, { 5: [1, 112] }, { 5: [2, 32] }, { 5: [2, 33] }, { 5: [2, 34] }, { 5: [1, 113] }, { 5: [2, 35] }, { 5: [2, 36] }, { 5: [2, 37] }, { 5: [2, 38] }, { 5: [1, 114] }, { 5: [2, 61] }, { 5: [2, 62] }, { 5: [1, 115] }, { 5: [2, 63] }, { 5: [2, 64] }, { 5: M, 29: 116, 30: P, 33: Y, 35: U, 37: B, 39: Q }, { 5: M, 29: 117, 30: P, 33: Y, 35: U, 37: B, 39: Q }, { 5: M, 29: 118, 30: P, 33: Y, 35: U, 37: B, 39: Q }, { 5: M, 29: 119, 30: P, 33: Y, 35: U, 37: B, 39: Q }, { 5: j, 39: X, 55: 120, 56: J, 58: Z }, { 5: j, 39: X, 55: 121, 56: J, 58: Z }, e(m, [2, 20]), e(m, [2, 21]), e(m, [2, 22]), e(m, [2, 23]), e(m, [2, 40]), e(m, [2, 41])],
    defaultActions: { 8: [2, 9], 10: [2, 2], 16: [2, 1], 37: [2, 3], 38: [2, 14], 39: [2, 15], 40: [2, 16], 41: [2, 17], 42: [2, 18], 44: [2, 53], 45: [2, 54], 47: [2, 59], 48: [2, 60], 52: [2, 11], 93: [2, 57], 94: [2, 58], 96: [2, 32], 97: [2, 33], 98: [2, 34], 100: [2, 35], 101: [2, 36], 102: [2, 37], 103: [2, 38], 105: [2, 61], 106: [2, 62], 108: [2, 63], 109: [2, 64] },
    parseError: function(r, s) {
      if (s.recoverable)
        this.trace(r);
      else {
        var l = new Error(r);
        throw l.hash = s, l;
      }
    },
    parse: function(r) {
      var s = this, l = [0], i = [], _ = [null], n = [], K = this.table, f = "", ee = 0, be = 0, Le = 2, ke = 1, Oe = n.slice.call(arguments, 1), E = Object.create(this.lexer), C = { yy: {} };
      for (var se in this.yy)
        Object.prototype.hasOwnProperty.call(this.yy, se) && (C.yy[se] = this.yy[se]);
      E.setInput(r, C.yy), C.yy.lexer = E, C.yy.parser = this, typeof E.yylloc > "u" && (E.yylloc = {});
      var ae = E.yylloc;
      n.push(ae);
      var Ce = E.options && E.options.ranges;
      typeof C.yy.parseError == "function" ? this.parseError = C.yy.parseError : this.parseError = Object.getPrototypeOf(this).parseError;
      function De() {
        var $;
        return $ = i.pop() || E.lex() || ke, typeof $ != "number" && ($ instanceof Array && (i = $, $ = i.pop()), $ = s.symbols_[$] || $), $;
      }
      for (var I, D, k, le, H = {}, te, V, Se, ie; ; ) {
        if (D = l[l.length - 1], this.defaultActions[D] ? k = this.defaultActions[D] : ((I === null || typeof I > "u") && (I = De()), k = K[D] && K[D][I]), typeof k > "u" || !k.length || !k[0]) {
          var ce = "";
          ie = [];
          for (te in K[D])
            this.terminals_[te] && te > Le && ie.push("'" + this.terminals_[te] + "'");
          E.showPosition ? ce = "Parse error on line " + (ee + 1) + `:
` + E.showPosition() + `
Expecting ` + ie.join(", ") + ", got '" + (this.terminals_[I] || I) + "'" : ce = "Parse error on line " + (ee + 1) + ": Unexpected " + (I == ke ? "end of input" : "'" + (this.terminals_[I] || I) + "'"), this.parseError(ce, {
            text: E.match,
            token: this.terminals_[I] || I,
            line: E.yylineno,
            loc: ae,
            expected: ie
          });
        }
        if (k[0] instanceof Array && k.length > 1)
          throw new Error("Parse Error: multiple actions possible at state: " + D + ", token: " + I);
        switch (k[0]) {
          case 1:
            l.push(I), _.push(E.yytext), n.push(E.yylloc), l.push(k[1]), I = null, be = E.yyleng, f = E.yytext, ee = E.yylineno, ae = E.yylloc;
            break;
          case 2:
            if (V = this.productions_[k[1]][1], H.$ = _[_.length - V], H._$ = {
              first_line: n[n.length - (V || 1)].first_line,
              last_line: n[n.length - 1].last_line,
              first_column: n[n.length - (V || 1)].first_column,
              last_column: n[n.length - 1].last_column
            }, Ce && (H._$.range = [
              n[n.length - (V || 1)].range[0],
              n[n.length - 1].range[1]
            ]), le = this.performAction.apply(H, [
              f,
              be,
              ee,
              C.yy,
              k[1],
              _,
              n
            ].concat(Oe)), typeof le < "u")
              return le;
            V && (l = l.slice(0, -1 * V * 2), _ = _.slice(0, -1 * V), n = n.slice(0, -1 * V)), l.push(this.productions_[k[1]][0]), _.push(H.$), n.push(H._$), Se = K[l[l.length - 2]][l[l.length - 1]], l.push(Se);
            break;
          case 3:
            return !0;
        }
      }
      return !0;
    }
  }, qe = function() {
    var q = {
      EOF: 1,
      parseError: function(s, l) {
        if (this.yy.parser)
          this.yy.parser.parseError(s, l);
        else
          throw new Error(s);
      },
      // resets the lexer, sets new input
      setInput: function(r, s) {
        return this.yy = s || this.yy || {}, this._input = r, this._more = this._backtrack = this.done = !1, this.yylineno = this.yyleng = 0, this.yytext = this.matched = this.match = "", this.conditionStack = ["INITIAL"], this.yylloc = {
          first_line: 1,
          first_column: 0,
          last_line: 1,
          last_column: 0
        }, this.options.ranges && (this.yylloc.range = [0, 0]), this.offset = 0, this;
      },
      // consumes and returns one char from the input
      input: function() {
        var r = this._input[0];
        this.yytext += r, this.yyleng++, this.offset++, this.match += r, this.matched += r;
        var s = r.match(/(?:\r\n?|\n).*/g);
        return s ? (this.yylineno++, this.yylloc.last_line++) : this.yylloc.last_column++, this.options.ranges && this.yylloc.range[1]++, this._input = this._input.slice(1), r;
      },
      // unshifts one char (or a string) into the input
      unput: function(r) {
        var s = r.length, l = r.split(/(?:\r\n?|\n)/g);
        this._input = r + this._input, this.yytext = this.yytext.substr(0, this.yytext.length - s), this.offset -= s;
        var i = this.match.split(/(?:\r\n?|\n)/g);
        this.match = this.match.substr(0, this.match.length - 1), this.matched = this.matched.substr(0, this.matched.length - 1), l.length - 1 && (this.yylineno -= l.length - 1);
        var _ = this.yylloc.range;
        return this.yylloc = {
          first_line: this.yylloc.first_line,
          last_line: this.yylineno + 1,
          first_column: this.yylloc.first_column,
          last_column: l ? (l.length === i.length ? this.yylloc.first_column : 0) + i[i.length - l.length].length - l[0].length : this.yylloc.first_column - s
        }, this.options.ranges && (this.yylloc.range = [_[0], _[0] + this.yyleng - s]), this.yyleng = this.yytext.length, this;
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
      less: function(r) {
        this.unput(this.match.slice(r));
      },
      // displays already matched input, i.e. for error messages
      pastInput: function() {
        var r = this.matched.substr(0, this.matched.length - this.match.length);
        return (r.length > 20 ? "..." : "") + r.substr(-20).replace(/\n/g, "");
      },
      // displays upcoming input, i.e. for error messages
      upcomingInput: function() {
        var r = this.match;
        return r.length < 20 && (r += this._input.substr(0, 20 - r.length)), (r.substr(0, 20) + (r.length > 20 ? "..." : "")).replace(/\n/g, "");
      },
      // displays the character position where the lexing error occurred, i.e. for error messages
      showPosition: function() {
        var r = this.pastInput(), s = new Array(r.length + 1).join("-");
        return r + this.upcomingInput() + `
` + s + "^";
      },
      // test the lexed token: return FALSE when not a match, otherwise return token
      test_match: function(r, s) {
        var l, i, _;
        if (this.options.backtrack_lexer && (_ = {
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
        }, this.options.ranges && (_.yylloc.range = this.yylloc.range.slice(0))), i = r[0].match(/(?:\r\n?|\n).*/g), i && (this.yylineno += i.length), this.yylloc = {
          first_line: this.yylloc.last_line,
          last_line: this.yylineno + 1,
          first_column: this.yylloc.last_column,
          last_column: i ? i[i.length - 1].length - i[i.length - 1].match(/\r?\n?/)[0].length : this.yylloc.last_column + r[0].length
        }, this.yytext += r[0], this.match += r[0], this.matches = r, this.yyleng = this.yytext.length, this.options.ranges && (this.yylloc.range = [this.offset, this.offset += this.yyleng]), this._more = !1, this._backtrack = !1, this._input = this._input.slice(r[0].length), this.matched += r[0], l = this.performAction.call(this, this.yy, this, s, this.conditionStack[this.conditionStack.length - 1]), this.done && this._input && (this.done = !1), l)
          return l;
        if (this._backtrack) {
          for (var n in _)
            this[n] = _[n];
          return !1;
        }
        return !1;
      },
      // return next match in input
      next: function() {
        if (this.done)
          return this.EOF;
        this._input || (this.done = !0);
        var r, s, l, i;
        this._more || (this.yytext = "", this.match = "");
        for (var _ = this._currentRules(), n = 0; n < _.length; n++)
          if (l = this._input.match(this.rules[_[n]]), l && (!s || l[0].length > s[0].length)) {
            if (s = l, i = n, this.options.backtrack_lexer) {
              if (r = this.test_match(l, _[n]), r !== !1)
                return r;
              if (this._backtrack) {
                s = !1;
                continue;
              } else
                return !1;
            } else if (!this.options.flex)
              break;
          }
        return s ? (r = this.test_match(s, _[i]), r !== !1 ? r : !1) : this._input === "" ? this.EOF : this.parseError("Lexical error on line " + (this.yylineno + 1) + `. Unrecognized text.
` + this.showPosition(), {
          text: "",
          token: null,
          line: this.yylineno
        });
      },
      // return next match that has a token
      lex: function() {
        var s = this.next();
        return s || this.lex();
      },
      // activates a new lexer condition state (pushes the new lexer condition state onto the condition stack)
      begin: function(s) {
        this.conditionStack.push(s);
      },
      // pop the previously active lexer condition state off the condition stack
      popState: function() {
        var s = this.conditionStack.length - 1;
        return s > 0 ? this.conditionStack.pop() : this.conditionStack[0];
      },
      // produce the lexer rule set which is active for the currently active lexer condition state
      _currentRules: function() {
        return this.conditionStack.length && this.conditionStack[this.conditionStack.length - 1] ? this.conditions[this.conditionStack[this.conditionStack.length - 1]].rules : this.conditions.INITIAL.rules;
      },
      // return the currently active lexer condition state; when an index argument is provided it produces the N-th previous condition state, if available
      topState: function(s) {
        return s = this.conditionStack.length - 1 - Math.abs(s || 0), s >= 0 ? this.conditionStack[s] : "INITIAL";
      },
      // alias for begin(condition)
      pushState: function(s) {
        this.begin(s);
      },
      // return the number of states currently on the stack
      stateStackSize: function() {
        return this.conditionStack.length;
      },
      options: { "case-insensitive": !0 },
      performAction: function(s, l, i, _) {
        switch (i) {
          case 0:
            return this.begin("open_directive"), 19;
          case 1:
            return this.begin("type_directive"), 20;
          case 2:
            return this.popState(), this.begin("arg_directive"), 12;
          case 3:
            return this.popState(), this.popState(), 22;
          case 4:
            return 21;
          case 5:
            return "title";
          case 6:
            return this.begin("acc_title"), 14;
          case 7:
            return this.popState(), "acc_title_value";
          case 8:
            return this.begin("acc_descr"), 16;
          case 9:
            return this.popState(), "acc_descr_value";
          case 10:
            this.begin("acc_descr_multiline");
            break;
          case 11:
            this.popState();
            break;
          case 12:
            return "acc_descr_multiline_value";
          case 13:
            return 5;
          case 14:
            break;
          case 15:
            break;
          case 16:
            break;
          case 17:
            return 8;
          case 18:
            return 6;
          case 19:
            return 28;
          case 20:
            return 39;
          case 21:
            return 31;
          case 22:
            return 30;
          case 23:
            return 33;
          case 24:
            return 35;
          case 25:
            return 37;
          case 26:
            return 40;
          case 27:
            return 41;
          case 28:
            return 42;
          case 29:
            return 43;
          case 30:
            return 44;
          case 31:
            return 45;
          case 32:
            return 46;
          case 33:
            return 47;
          case 34:
            return 48;
          case 35:
            return 49;
          case 36:
            return 50;
          case 37:
            return 51;
          case 38:
            return 52;
          case 39:
            return 53;
          case 40:
            return 64;
          case 41:
            return 65;
          case 42:
            return 66;
          case 43:
            return 67;
          case 44:
            return 68;
          case 45:
            return 69;
          case 46:
            return 70;
          case 47:
            return 56;
          case 48:
            return 58;
          case 49:
            return 60;
          case 50:
            return 63;
          case 51:
            return 62;
          case 52:
            this.begin("string");
            break;
          case 53:
            this.popState();
            break;
          case 54:
            return "qString";
          case 55:
            return l.yytext = l.yytext.trim(), 71;
        }
      },
      rules: [/^(?:%%\{)/i, /^(?:((?:(?!\}%%)[^:.])*))/i, /^(?::)/i, /^(?:\}%%)/i, /^(?:((?:(?!\}%%).|\n)*))/i, /^(?:title\s[^#\n;]+)/i, /^(?:accTitle\s*:\s*)/i, /^(?:(?!\n||)*[^\n]*)/i, /^(?:accDescr\s*:\s*)/i, /^(?:(?!\n||)*[^\n]*)/i, /^(?:accDescr\s*\{\s*)/i, /^(?:[\}])/i, /^(?:[^\}]*)/i, /^(?:(\r?\n)+)/i, /^(?:\s+)/i, /^(?:#[^\n]*)/i, /^(?:%[^\n]*)/i, /^(?:$)/i, /^(?:requirementDiagram\b)/i, /^(?:\{)/i, /^(?:\})/i, /^(?::)/i, /^(?:id\b)/i, /^(?:text\b)/i, /^(?:risk\b)/i, /^(?:verifyMethod\b)/i, /^(?:requirement\b)/i, /^(?:functionalRequirement\b)/i, /^(?:interfaceRequirement\b)/i, /^(?:performanceRequirement\b)/i, /^(?:physicalRequirement\b)/i, /^(?:designConstraint\b)/i, /^(?:low\b)/i, /^(?:medium\b)/i, /^(?:high\b)/i, /^(?:analysis\b)/i, /^(?:demonstration\b)/i, /^(?:inspection\b)/i, /^(?:test\b)/i, /^(?:element\b)/i, /^(?:contains\b)/i, /^(?:copies\b)/i, /^(?:derives\b)/i, /^(?:satisfies\b)/i, /^(?:verifies\b)/i, /^(?:refines\b)/i, /^(?:traces\b)/i, /^(?:type\b)/i, /^(?:docref\b)/i, /^(?:<-)/i, /^(?:->)/i, /^(?:-)/i, /^(?:["])/i, /^(?:["])/i, /^(?:[^"]*)/i, /^(?:[\w][^\r\n\{\<\>\-\=]*)/i],
      conditions: { acc_descr_multiline: { rules: [11, 12], inclusive: !1 }, acc_descr: { rules: [9], inclusive: !1 }, acc_title: { rules: [7], inclusive: !1 }, close_directive: { rules: [], inclusive: !1 }, arg_directive: { rules: [3, 4], inclusive: !1 }, type_directive: { rules: [2, 3], inclusive: !1 }, open_directive: { rules: [1], inclusive: !1 }, unqString: { rules: [], inclusive: !1 }, token: { rules: [], inclusive: !1 }, string: { rules: [53, 54], inclusive: !1 }, INITIAL: { rules: [0, 5, 6, 8, 10, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 55], inclusive: !0 } }
    };
    return q;
  }();
  re.lexer = qe;
  function ne() {
    this.yy = {};
  }
  return ne.prototype = re, re.Parser = ne, new ne();
}();
he.parser = he;
const Ge = he;
let de = [], b = {}, G = {}, L = {}, z = {};
const ze = {
  REQUIREMENT: "Requirement",
  FUNCTIONAL_REQUIREMENT: "Functional Requirement",
  INTERFACE_REQUIREMENT: "Interface Requirement",
  PERFORMANCE_REQUIREMENT: "Performance Requirement",
  PHYSICAL_REQUIREMENT: "Physical Requirement",
  DESIGN_CONSTRAINT: "Design Constraint"
}, je = {
  LOW_RISK: "Low",
  MED_RISK: "Medium",
  HIGH_RISK: "High"
}, Xe = {
  VERIFY_ANALYSIS: "Analysis",
  VERIFY_DEMONSTRATION: "Demonstration",
  VERIFY_INSPECTION: "Inspection",
  VERIFY_TEST: "Test"
}, Je = {
  CONTAINS: "contains",
  COPIES: "copies",
  DERIVES: "derives",
  SATISFIES: "satisfies",
  VERIFIES: "verifies",
  REFINES: "refines",
  TRACES: "traces"
}, Ze = function(e, t, a) {
  Be.parseDirective(this, e, t, a);
}, et = (e, t) => (G[e] === void 0 && (G[e] = {
  name: e,
  type: t,
  id: b.id,
  text: b.text,
  risk: b.risk,
  verifyMethod: b.verifyMethod
}), b = {}, G[e]), tt = () => G, it = (e) => {
  b !== void 0 && (b.id = e);
}, rt = (e) => {
  b !== void 0 && (b.text = e);
}, nt = (e) => {
  b !== void 0 && (b.risk = e);
}, st = (e) => {
  b !== void 0 && (b.verifyMethod = e);
}, at = (e) => (z[e] === void 0 && (z[e] = {
  name: e,
  type: L.type,
  docRef: L.docRef
}, ve.info("Added new requirement: ", e)), L = {}, z[e]), lt = () => z, ct = (e) => {
  L !== void 0 && (L.type = e);
}, ot = (e) => {
  L !== void 0 && (L.docRef = e);
}, ht = (e, t, a) => {
  de.push({
    type: e,
    src: t,
    dst: a
  });
}, ut = () => de, dt = () => {
  de = [], b = {}, G = {}, L = {}, z = {}, Ue();
}, pt = {
  RequirementType: ze,
  RiskLevel: je,
  VerifyType: Xe,
  Relationships: Je,
  parseDirective: Ze,
  getConfig: () => xe().req,
  addRequirement: et,
  getRequirements: tt,
  setNewReqId: it,
  setNewReqText: rt,
  setNewReqRisk: nt,
  setNewReqVerifyMethod: st,
  setAccTitle: Fe,
  getAccTitle: Me,
  setAccDescription: Pe,
  getAccDescription: Ye,
  addElement: at,
  getElements: lt,
  setNewElementType: ct,
  setNewElementDocRef: ot,
  addRelationship: ht,
  getRelationships: ut,
  clear: dt
}, ft = (e) => `

  marker {
    fill: ${e.relationColor};
    stroke: ${e.relationColor};
  }

  marker.cross {
    stroke: ${e.lineColor};
  }

  svg {
    font-family: ${e.fontFamily};
    font-size: ${e.fontSize};
  }

  .reqBox {
    fill: ${e.requirementBackground};
    fill-opacity: 100%;
    stroke: ${e.requirementBorderColor};
    stroke-width: ${e.requirementBorderSize};
  }
  
  .reqTitle, .reqLabel{
    fill:  ${e.requirementTextColor};
  }
  .reqLabelBox {
    fill: ${e.relationLabelBackground};
    fill-opacity: 100%;
  }

  .req-title-line {
    stroke: ${e.requirementBorderColor};
    stroke-width: ${e.requirementBorderSize};
  }
  .relationshipLine {
    stroke: ${e.relationColor};
    stroke-width: 1;
  }
  .relationshipLabel {
    fill: ${e.relationLabelColor};
  }

`, _t = ft, ue = {
  CONTAINS: "contains",
  ARROW: "arrow"
}, yt = (e, t) => {
  let a = e.append("defs").append("marker").attr("id", ue.CONTAINS + "_line_ending").attr("refX", 0).attr("refY", t.line_height / 2).attr("markerWidth", t.line_height).attr("markerHeight", t.line_height).attr("orient", "auto").append("g");
  a.append("circle").attr("cx", t.line_height / 2).attr("cy", t.line_height / 2).attr("r", t.line_height / 2).attr("fill", "none"), a.append("line").attr("x1", 0).attr("x2", t.line_height).attr("y1", t.line_height / 2).attr("y2", t.line_height / 2).attr("stroke-width", 1), a.append("line").attr("y1", 0).attr("y2", t.line_height).attr("x1", t.line_height / 2).attr("x2", t.line_height / 2).attr("stroke-width", 1), e.append("defs").append("marker").attr("id", ue.ARROW + "_line_ending").attr("refX", t.line_height).attr("refY", 0.5 * t.line_height).attr("markerWidth", t.line_height).attr("markerHeight", t.line_height).attr("orient", "auto").append("path").attr(
    "d",
    `M0,0
      L${t.line_height},${t.line_height / 2}
      M${t.line_height},${t.line_height / 2}
      L0,${t.line_height}`
  ).attr("stroke-width", 1);
}, Ae = {
  ReqMarkers: ue,
  insertLineEndings: yt
};
let y = {}, Ne = 0;
const we = (e, t) => e.insert("rect", "#" + t).attr("class", "req reqBox").attr("x", 0).attr("y", 0).attr("width", y.rect_min_width + "px").attr("height", y.rect_min_height + "px"), Ve = (e, t, a) => {
  let c = y.rect_min_width / 2, d = e.append("text").attr("class", "req reqLabel reqTitle").attr("id", t).attr("x", c).attr("y", y.rect_padding).attr("dominant-baseline", "hanging"), u = 0;
  a.forEach((g) => {
    u == 0 ? d.append("tspan").attr("text-anchor", "middle").attr("x", y.rect_min_width / 2).attr("dy", 0).text(g) : d.append("tspan").attr("text-anchor", "middle").attr("x", y.rect_min_width / 2).attr("dy", y.line_height * 0.75).text(g), u++;
  });
  let p = 1.5 * y.rect_padding, h = u * y.line_height * 0.75, o = p + h;
  return e.append("line").attr("class", "req-title-line").attr("x1", "0").attr("x2", y.rect_min_width).attr("y1", o).attr("y2", o), {
    titleNode: d,
    y: o
  };
}, $e = (e, t, a, c) => {
  let d = e.append("text").attr("class", "req reqLabel").attr("id", t).attr("x", y.rect_padding).attr("y", c).attr("dominant-baseline", "hanging"), u = 0;
  const p = 30;
  let h = [];
  return a.forEach((o) => {
    let g = o.length;
    for (; g > p && u < 3; ) {
      let R = o.substring(0, p);
      o = o.substring(p, o.length), g = o.length, h[h.length] = R, u++;
    }
    if (u == 3) {
      let R = h[h.length - 1];
      h[h.length - 1] = R.substring(0, R.length - 4) + "...";
    } else
      h[h.length] = o;
    u = 0;
  }), h.forEach((o) => {
    d.append("tspan").attr("x", y.rect_padding).attr("dy", y.line_height).text(o);
  }), d;
}, gt = (e, t, a, c) => {
  const d = t.node().getTotalLength(), u = t.node().getPointAtLength(d * 0.5), p = "rel" + Ne;
  Ne++;
  const o = e.append("text").attr("class", "req relationshipLabel").attr("id", p).attr("x", u.x).attr("y", u.y).attr("text-anchor", "middle").attr("dominant-baseline", "middle").text(c).node().getBBox();
  e.insert("rect", "#" + p).attr("class", "req reqLabelBox").attr("x", u.x - o.width / 2).attr("y", u.y - o.height / 2).attr("width", o.width).attr("height", o.height).attr("fill", "white").attr("fill-opacity", "85%");
}, Et = function(e, t, a, c, d) {
  const u = a.edge(W(t.src), W(t.dst)), p = Ke().x(function(o) {
    return o.x;
  }).y(function(o) {
    return o.y;
  }), h = e.insert("path", "#" + c).attr("class", "er relationshipLine").attr("d", p(u.points)).attr("fill", "none");
  t.type == d.db.Relationships.CONTAINS ? h.attr(
    "marker-start",
    "url(" + Te.getUrl(y.arrowMarkerAbsolute) + "#" + t.type + "_line_ending)"
  ) : (h.attr("stroke-dasharray", "10,7"), h.attr(
    "marker-end",
    "url(" + Te.getUrl(y.arrowMarkerAbsolute) + "#" + Ae.ReqMarkers.ARROW + "_line_ending)"
  )), gt(e, h, y, `<<${t.type}>>`);
}, Rt = (e, t, a) => {
  Object.keys(e).forEach((c) => {
    let d = e[c];
    c = W(c), ve.info("Added new requirement: ", c);
    const u = a.append("g").attr("id", c), p = "req-" + c, h = we(u, p);
    let o = Ve(u, c + "_title", [
      `<<${d.type}>>`,
      `${d.name}`
    ]);
    $e(
      u,
      c + "_body",
      [
        `Id: ${d.id}`,
        `Text: ${d.text}`,
        `Risk: ${d.risk}`,
        `Verification: ${d.verifyMethod}`
      ],
      o.y
    );
    const g = h.node().getBBox();
    t.setNode(c, {
      width: g.width,
      height: g.height,
      shape: "rect",
      id: c
    });
  });
}, mt = (e, t, a) => {
  Object.keys(e).forEach((c) => {
    let d = e[c];
    const u = W(c), p = a.append("g").attr("id", u), h = "element-" + u, o = we(p, h);
    let g = Ve(p, h + "_title", ["<<Element>>", `${c}`]);
    $e(
      p,
      h + "_body",
      [`Type: ${d.type || "Not Specified"}`, `Doc Ref: ${d.docRef || "None"}`],
      g.y
    );
    const R = o.node().getBBox();
    t.setNode(u, {
      width: R.width,
      height: R.height,
      shape: "rect",
      id: u
    });
  });
}, It = (e, t) => (e.forEach(function(a) {
  let c = W(a.src), d = W(a.dst);
  t.setEdge(c, d, { relationship: a });
}), e), bt = function(e, t) {
  t.nodes().forEach(function(a) {
    a !== void 0 && t.node(a) !== void 0 && (e.select("#" + a), e.select("#" + a).attr(
      "transform",
      "translate(" + (t.node(a).x - t.node(a).width / 2) + "," + (t.node(a).y - t.node(a).height / 2) + " )"
    ));
  });
}, W = (e) => e.replace(/\s/g, "").replace(/\./g, "_"), kt = (e, t, a, c) => {
  y = xe().requirement, c.db.clear(), c.parser.parse(e);
  const d = y.securityLevel;
  let u;
  d === "sandbox" && (u = oe("#i" + t));
  const h = (d === "sandbox" ? oe(u.nodes()[0].contentDocument.body) : oe("body")).select(`[id='${t}']`);
  Ae.insertLineEndings(h, y);
  const o = new Qe({
    multigraph: !1,
    compound: !1,
    directed: !0
  }).setGraph({
    rankdir: y.layoutDirection,
    marginx: 20,
    marginy: 20,
    nodesep: 100,
    edgesep: 100,
    ranksep: 100
  }).setDefaultEdgeLabel(function() {
    return {};
  });
  let g = c.db.getRequirements(), R = c.db.getElements(), x = c.db.getRelationships();
  Rt(g, o, h), mt(R, o, h), It(x, o), He(o), bt(h, o), x.forEach(function(N) {
    Et(h, N, o, t, c);
  });
  const S = y.rect_padding, T = h.node().getBBox(), v = T.width + S * 2, A = T.height + S * 2;
  We(h, A, v, y.useMaxWidth), h.attr("viewBox", `${T.x - S} ${T.y - S} ${v} ${A}`);
}, St = {
  draw: kt
}, $t = {
  parser: Ge,
  db: pt,
  renderer: St,
  styles: _t
};
export {
  $t as diagram
};
//# sourceMappingURL=requirementDiagram-51a5ec78.js.map
