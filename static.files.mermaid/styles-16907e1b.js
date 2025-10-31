import { select } from "d3";
import { o as setAccTitle, p as getAccTitle, q as getAccDescription, v as setAccDescription, g as getConfig, w as setDiagramTitle, x as getDiagramTitle, y as clear$1, l as log, e as common } from "./commonDb-573409be.js";
import { u as utils } from "./utils-d622194a.js";
import { m as mermaidAPI } from "./mermaidAPI-3ae0f2f0.js";
var parser = function() {
  var o = function(k, v, o2, l) {
    for (o2 = o2 || {}, l = k.length; l--; o2[k[l]] = v)
      ;
    return o2;
  }, $V0 = [1, 3], $V1 = [1, 7], $V2 = [1, 8], $V3 = [1, 9], $V4 = [1, 10], $V5 = [1, 13], $V6 = [1, 12], $V7 = [1, 16, 25], $V8 = [1, 20], $V9 = [1, 32], $Va = [1, 33], $Vb = [1, 34], $Vc = [1, 48], $Vd = [1, 39], $Ve = [1, 37], $Vf = [1, 38], $Vg = [1, 44], $Vh = [1, 45], $Vi = [1, 40], $Vj = [1, 41], $Vk = [1, 42], $Vl = [1, 43], $Vm = [1, 49], $Vn = [1, 50], $Vo = [1, 51], $Vp = [1, 52], $Vq = [16, 25], $Vr = [1, 66], $Vs = [1, 67], $Vt = [1, 68], $Vu = [1, 69], $Vv = [1, 70], $Vw = [1, 71], $Vx = [1, 72], $Vy = [1, 82], $Vz = [16, 25, 28, 29, 36, 49, 50, 64, 65, 66, 67, 68, 69, 70, 75, 77], $VA = [16, 25, 28, 29, 34, 36, 49, 50, 55, 64, 65, 66, 67, 68, 69, 70, 75, 77, 92, 93, 94, 95], $VB = [5, 8, 9, 10, 11, 16, 19, 23, 25], $VC = [29, 92, 93, 94, 95], $VD = [29, 69, 70, 92, 93, 94, 95], $VE = [29, 64, 65, 66, 67, 68, 92, 93, 94, 95], $VF = [1, 96], $VG = [16, 25, 49, 50], $VH = [16, 25, 36];
  var parser2 = {
    trace: function trace() {
    },
    yy: {},
    symbols_: { "error": 2, "start": 3, "mermaidDoc": 4, "statments": 5, "direction": 6, "directive": 7, "direction_tb": 8, "direction_bt": 9, "direction_rl": 10, "direction_lr": 11, "graphConfig": 12, "openDirective": 13, "typeDirective": 14, "closeDirective": 15, "NEWLINE": 16, ":": 17, "argDirective": 18, "open_directive": 19, "type_directive": 20, "arg_directive": 21, "close_directive": 22, "CLASS_DIAGRAM": 23, "statements": 24, "EOF": 25, "statement": 26, "classLabel": 27, "SQS": 28, "STR": 29, "SQE": 30, "className": 31, "alphaNumToken": 32, "classLiteralName": 33, "GENERICTYPE": 34, "relationStatement": 35, "LABEL": 36, "classStatement": 37, "methodStatement": 38, "annotationStatement": 39, "clickStatement": 40, "cssClassStatement": 41, "noteStatement": 42, "acc_title": 43, "acc_title_value": 44, "acc_descr": 45, "acc_descr_value": 46, "acc_descr_multiline_value": 47, "classIdentifier": 48, "STYLE_SEPARATOR": 49, "STRUCT_START": 50, "members": 51, "STRUCT_STOP": 52, "CLASS": 53, "ANNOTATION_START": 54, "ANNOTATION_END": 55, "MEMBER": 56, "SEPARATOR": 57, "relation": 58, "NOTE_FOR": 59, "noteText": 60, "NOTE": 61, "relationType": 62, "lineType": 63, "AGGREGATION": 64, "EXTENSION": 65, "COMPOSITION": 66, "DEPENDENCY": 67, "LOLLIPOP": 68, "LINE": 69, "DOTTED_LINE": 70, "CALLBACK": 71, "LINK": 72, "LINK_TARGET": 73, "CLICK": 74, "CALLBACK_NAME": 75, "CALLBACK_ARGS": 76, "HREF": 77, "CSSCLASS": 78, "commentToken": 79, "textToken": 80, "graphCodeTokens": 81, "textNoTagsToken": 82, "TAGSTART": 83, "TAGEND": 84, "==": 85, "--": 86, "PCT": 87, "DEFAULT": 88, "SPACE": 89, "MINUS": 90, "keywords": 91, "UNICODE_TEXT": 92, "NUM": 93, "ALPHA": 94, "BQUOTE_STR": 95, "$accept": 0, "$end": 1 },
    terminals_: { 2: "error", 5: "statments", 8: "direction_tb", 9: "direction_bt", 10: "direction_rl", 11: "direction_lr", 16: "NEWLINE", 17: ":", 19: "open_directive", 20: "type_directive", 21: "arg_directive", 22: "close_directive", 23: "CLASS_DIAGRAM", 25: "EOF", 28: "SQS", 29: "STR", 30: "SQE", 34: "GENERICTYPE", 36: "LABEL", 43: "acc_title", 44: "acc_title_value", 45: "acc_descr", 46: "acc_descr_value", 47: "acc_descr_multiline_value", 49: "STYLE_SEPARATOR", 50: "STRUCT_START", 52: "STRUCT_STOP", 53: "CLASS", 54: "ANNOTATION_START", 55: "ANNOTATION_END", 56: "MEMBER", 57: "SEPARATOR", 59: "NOTE_FOR", 61: "NOTE", 64: "AGGREGATION", 65: "EXTENSION", 66: "COMPOSITION", 67: "DEPENDENCY", 68: "LOLLIPOP", 69: "LINE", 70: "DOTTED_LINE", 71: "CALLBACK", 72: "LINK", 73: "LINK_TARGET", 74: "CLICK", 75: "CALLBACK_NAME", 76: "CALLBACK_ARGS", 77: "HREF", 78: "CSSCLASS", 81: "graphCodeTokens", 83: "TAGSTART", 84: "TAGEND", 85: "==", 86: "--", 87: "PCT", 88: "DEFAULT", 89: "SPACE", 90: "MINUS", 91: "keywords", 92: "UNICODE_TEXT", 93: "NUM", 94: "ALPHA", 95: "BQUOTE_STR" },
    productions_: [0, [3, 1], [3, 1], [3, 1], [3, 2], [6, 1], [6, 1], [6, 1], [6, 1], [4, 1], [7, 4], [7, 6], [13, 1], [14, 1], [18, 1], [15, 1], [12, 4], [24, 1], [24, 2], [24, 3], [27, 3], [31, 1], [31, 1], [31, 2], [31, 2], [31, 2], [26, 1], [26, 2], [26, 1], [26, 1], [26, 1], [26, 1], [26, 1], [26, 1], [26, 1], [26, 1], [26, 2], [26, 2], [26, 1], [37, 1], [37, 3], [37, 4], [37, 6], [48, 2], [48, 3], [39, 4], [51, 1], [51, 2], [38, 1], [38, 2], [38, 1], [38, 1], [35, 3], [35, 4], [35, 4], [35, 5], [42, 3], [42, 2], [58, 3], [58, 2], [58, 2], [58, 1], [62, 1], [62, 1], [62, 1], [62, 1], [62, 1], [63, 1], [63, 1], [40, 3], [40, 4], [40, 3], [40, 4], [40, 4], [40, 5], [40, 3], [40, 4], [40, 4], [40, 5], [40, 3], [40, 4], [40, 4], [40, 5], [41, 3], [79, 1], [79, 1], [80, 1], [80, 1], [80, 1], [80, 1], [80, 1], [80, 1], [80, 1], [82, 1], [82, 1], [82, 1], [82, 1], [32, 1], [32, 1], [32, 1], [33, 1], [60, 1]],
    performAction: function anonymous(yytext, yyleng, yylineno, yy, yystate, $$, _$) {
      var $0 = $$.length - 1;
      switch (yystate) {
        case 5:
          yy.setDirection("TB");
          break;
        case 6:
          yy.setDirection("BT");
          break;
        case 7:
          yy.setDirection("RL");
          break;
        case 8:
          yy.setDirection("LR");
          break;
        case 12:
          yy.parseDirective("%%{", "open_directive");
          break;
        case 13:
          yy.parseDirective($$[$0], "type_directive");
          break;
        case 14:
          $$[$0] = $$[$0].trim().replace(/'/g, '"');
          yy.parseDirective($$[$0], "arg_directive");
          break;
        case 15:
          yy.parseDirective("}%%", "close_directive", "class");
          break;
        case 20:
          this.$ = $$[$0 - 1];
          break;
        case 21:
        case 22:
          this.$ = $$[$0];
          break;
        case 23:
          this.$ = $$[$0 - 1] + $$[$0];
          break;
        case 24:
        case 25:
          this.$ = $$[$0 - 1] + "~" + $$[$0];
          break;
        case 26:
          yy.addRelation($$[$0]);
          break;
        case 27:
          $$[$0 - 1].title = yy.cleanupLabel($$[$0]);
          yy.addRelation($$[$0 - 1]);
          break;
        case 36:
          this.$ = $$[$0].trim();
          yy.setAccTitle(this.$);
          break;
        case 37:
        case 38:
          this.$ = $$[$0].trim();
          yy.setAccDescription(this.$);
          break;
        case 40:
          yy.setCssClass($$[$0 - 2], $$[$0]);
          break;
        case 41:
          yy.addMembers($$[$0 - 3], $$[$0 - 1]);
          break;
        case 42:
          yy.setCssClass($$[$0 - 5], $$[$0 - 3]);
          yy.addMembers($$[$0 - 5], $$[$0 - 1]);
          break;
        case 43:
          this.$ = $$[$0];
          yy.addClass($$[$0]);
          break;
        case 44:
          this.$ = $$[$0 - 1];
          yy.addClass($$[$0 - 1]);
          yy.setClassLabel($$[$0 - 1], $$[$0]);
          break;
        case 45:
          yy.addAnnotation($$[$0], $$[$0 - 2]);
          break;
        case 46:
          this.$ = [$$[$0]];
          break;
        case 47:
          $$[$0].push($$[$0 - 1]);
          this.$ = $$[$0];
          break;
        case 48:
          break;
        case 49:
          yy.addMember($$[$0 - 1], yy.cleanupLabel($$[$0]));
          break;
        case 50:
          break;
        case 51:
          break;
        case 52:
          this.$ = { "id1": $$[$0 - 2], "id2": $$[$0], relation: $$[$0 - 1], relationTitle1: "none", relationTitle2: "none" };
          break;
        case 53:
          this.$ = { id1: $$[$0 - 3], id2: $$[$0], relation: $$[$0 - 1], relationTitle1: $$[$0 - 2], relationTitle2: "none" };
          break;
        case 54:
          this.$ = { id1: $$[$0 - 3], id2: $$[$0], relation: $$[$0 - 2], relationTitle1: "none", relationTitle2: $$[$0 - 1] };
          break;
        case 55:
          this.$ = { id1: $$[$0 - 4], id2: $$[$0], relation: $$[$0 - 2], relationTitle1: $$[$0 - 3], relationTitle2: $$[$0 - 1] };
          break;
        case 56:
          yy.addNote($$[$0], $$[$0 - 1]);
          break;
        case 57:
          yy.addNote($$[$0]);
          break;
        case 58:
          this.$ = { type1: $$[$0 - 2], type2: $$[$0], lineType: $$[$0 - 1] };
          break;
        case 59:
          this.$ = { type1: "none", type2: $$[$0], lineType: $$[$0 - 1] };
          break;
        case 60:
          this.$ = { type1: $$[$0 - 1], type2: "none", lineType: $$[$0] };
          break;
        case 61:
          this.$ = { type1: "none", type2: "none", lineType: $$[$0] };
          break;
        case 62:
          this.$ = yy.relationType.AGGREGATION;
          break;
        case 63:
          this.$ = yy.relationType.EXTENSION;
          break;
        case 64:
          this.$ = yy.relationType.COMPOSITION;
          break;
        case 65:
          this.$ = yy.relationType.DEPENDENCY;
          break;
        case 66:
          this.$ = yy.relationType.LOLLIPOP;
          break;
        case 67:
          this.$ = yy.lineType.LINE;
          break;
        case 68:
          this.$ = yy.lineType.DOTTED_LINE;
          break;
        case 69:
        case 75:
          this.$ = $$[$0 - 2];
          yy.setClickEvent($$[$0 - 1], $$[$0]);
          break;
        case 70:
        case 76:
          this.$ = $$[$0 - 3];
          yy.setClickEvent($$[$0 - 2], $$[$0 - 1]);
          yy.setTooltip($$[$0 - 2], $$[$0]);
          break;
        case 71:
        case 79:
          this.$ = $$[$0 - 2];
          yy.setLink($$[$0 - 1], $$[$0]);
          break;
        case 72:
          this.$ = $$[$0 - 3];
          yy.setLink($$[$0 - 2], $$[$0 - 1], $$[$0]);
          break;
        case 73:
        case 81:
          this.$ = $$[$0 - 3];
          yy.setLink($$[$0 - 2], $$[$0 - 1]);
          yy.setTooltip($$[$0 - 2], $$[$0]);
          break;
        case 74:
        case 82:
          this.$ = $$[$0 - 4];
          yy.setLink($$[$0 - 3], $$[$0 - 2], $$[$0]);
          yy.setTooltip($$[$0 - 3], $$[$0 - 1]);
          break;
        case 77:
          this.$ = $$[$0 - 3];
          yy.setClickEvent($$[$0 - 2], $$[$0 - 1], $$[$0]);
          break;
        case 78:
          this.$ = $$[$0 - 4];
          yy.setClickEvent($$[$0 - 3], $$[$0 - 2], $$[$0 - 1]);
          yy.setTooltip($$[$0 - 3], $$[$0]);
          break;
        case 80:
          this.$ = $$[$0 - 3];
          yy.setLink($$[$0 - 2], $$[$0 - 1], $$[$0]);
          break;
        case 83:
          yy.setCssClass($$[$0 - 1], $$[$0]);
          break;
      }
    },
    table: [{ 3: 1, 4: 2, 5: $V0, 6: 4, 7: 5, 8: $V1, 9: $V2, 10: $V3, 11: $V4, 12: 6, 13: 11, 19: $V5, 23: $V6 }, { 1: [3] }, { 1: [2, 1] }, { 1: [2, 2] }, { 1: [2, 3] }, { 3: 14, 4: 2, 5: $V0, 6: 4, 7: 5, 8: $V1, 9: $V2, 10: $V3, 11: $V4, 12: 6, 13: 11, 19: $V5, 23: $V6 }, { 1: [2, 9] }, o($V7, [2, 5]), o($V7, [2, 6]), o($V7, [2, 7]), o($V7, [2, 8]), { 14: 15, 20: [1, 16] }, { 16: [1, 17] }, { 20: [2, 12] }, { 1: [2, 4] }, { 15: 18, 17: [1, 19], 22: $V8 }, o([17, 22], [2, 13]), { 6: 31, 7: 30, 8: $V1, 9: $V2, 10: $V3, 11: $V4, 13: 11, 19: $V5, 24: 21, 26: 22, 31: 35, 32: 46, 33: 47, 35: 23, 37: 24, 38: 25, 39: 26, 40: 27, 41: 28, 42: 29, 43: $V9, 45: $Va, 47: $Vb, 48: 36, 53: $Vc, 54: $Vd, 56: $Ve, 57: $Vf, 59: $Vg, 61: $Vh, 71: $Vi, 72: $Vj, 74: $Vk, 78: $Vl, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, { 16: [1, 53] }, { 18: 54, 21: [1, 55] }, { 16: [2, 15] }, { 25: [1, 56] }, { 16: [1, 57], 25: [2, 17] }, o($Vq, [2, 26], { 36: [1, 58] }), o($Vq, [2, 28]), o($Vq, [2, 29]), o($Vq, [2, 30]), o($Vq, [2, 31]), o($Vq, [2, 32]), o($Vq, [2, 33]), o($Vq, [2, 34]), o($Vq, [2, 35]), { 44: [1, 59] }, { 46: [1, 60] }, o($Vq, [2, 38]), o($Vq, [2, 48], { 58: 61, 62: 64, 63: 65, 29: [1, 62], 36: [1, 63], 64: $Vr, 65: $Vs, 66: $Vt, 67: $Vu, 68: $Vv, 69: $Vw, 70: $Vx }), o($Vq, [2, 39], { 49: [1, 73], 50: [1, 74] }), o($Vq, [2, 50]), o($Vq, [2, 51]), { 32: 75, 92: $Vm, 93: $Vn, 94: $Vo }, { 31: 76, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, { 31: 77, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, { 31: 78, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, { 29: [1, 79] }, { 31: 80, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, { 29: $Vy, 60: 81 }, o($Vz, [2, 21], { 32: 46, 33: 47, 31: 83, 34: [1, 84], 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }), o($Vz, [2, 22], { 34: [1, 85] }), { 31: 86, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, o($VA, [2, 97]), o($VA, [2, 98]), o($VA, [2, 99]), o([16, 25, 28, 29, 34, 36, 49, 50, 64, 65, 66, 67, 68, 69, 70, 75, 77], [2, 100]), o($VB, [2, 10]), { 15: 87, 22: $V8 }, { 22: [2, 14] }, { 1: [2, 16] }, { 6: 31, 7: 30, 8: $V1, 9: $V2, 10: $V3, 11: $V4, 13: 11, 19: $V5, 24: 88, 25: [2, 18], 26: 22, 31: 35, 32: 46, 33: 47, 35: 23, 37: 24, 38: 25, 39: 26, 40: 27, 41: 28, 42: 29, 43: $V9, 45: $Va, 47: $Vb, 48: 36, 53: $Vc, 54: $Vd, 56: $Ve, 57: $Vf, 59: $Vg, 61: $Vh, 71: $Vi, 72: $Vj, 74: $Vk, 78: $Vl, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, o($Vq, [2, 27]), o($Vq, [2, 36]), o($Vq, [2, 37]), { 29: [1, 90], 31: 89, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, { 58: 91, 62: 64, 63: 65, 64: $Vr, 65: $Vs, 66: $Vt, 67: $Vu, 68: $Vv, 69: $Vw, 70: $Vx }, o($Vq, [2, 49]), { 63: 92, 69: $Vw, 70: $Vx }, o($VC, [2, 61], { 62: 93, 64: $Vr, 65: $Vs, 66: $Vt, 67: $Vu, 68: $Vv }), o($VD, [2, 62]), o($VD, [2, 63]), o($VD, [2, 64]), o($VD, [2, 65]), o($VD, [2, 66]), o($VE, [2, 67]), o($VE, [2, 68]), { 32: 94, 92: $Vm, 93: $Vn, 94: $Vo }, { 51: 95, 56: $VF }, { 55: [1, 97] }, { 29: [1, 98] }, { 29: [1, 99] }, { 75: [1, 100], 77: [1, 101] }, { 32: 102, 92: $Vm, 93: $Vn, 94: $Vo }, { 29: $Vy, 60: 103 }, o($Vq, [2, 57]), o($Vq, [2, 101]), o($Vz, [2, 23]), o($Vz, [2, 24]), o($Vz, [2, 25]), o($VG, [2, 43], { 27: 104, 28: [1, 105] }), { 16: [1, 106] }, { 25: [2, 19] }, o($VH, [2, 52]), { 31: 107, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, { 29: [1, 109], 31: 108, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, o($VC, [2, 60], { 62: 110, 64: $Vr, 65: $Vs, 66: $Vt, 67: $Vu, 68: $Vv }), o($VC, [2, 59]), o($Vq, [2, 40], { 50: [1, 111] }), { 52: [1, 112] }, { 51: 113, 52: [2, 46], 56: $VF }, { 31: 114, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, o($Vq, [2, 69], { 29: [1, 115] }), o($Vq, [2, 71], { 29: [1, 117], 73: [1, 116] }), o($Vq, [2, 75], { 29: [1, 118], 76: [1, 119] }), o($Vq, [2, 79], { 29: [1, 121], 73: [1, 120] }), o($Vq, [2, 83]), o($Vq, [2, 56]), o($VG, [2, 44]), { 29: [1, 122] }, o($VB, [2, 11]), o($VH, [2, 54]), o($VH, [2, 53]), { 31: 123, 32: 46, 33: 47, 92: $Vm, 93: $Vn, 94: $Vo, 95: $Vp }, o($VC, [2, 58]), { 51: 124, 56: $VF }, o($Vq, [2, 41]), { 52: [2, 47] }, o($Vq, [2, 45]), o($Vq, [2, 70]), o($Vq, [2, 72]), o($Vq, [2, 73], { 73: [1, 125] }), o($Vq, [2, 76]), o($Vq, [2, 77], { 29: [1, 126] }), o($Vq, [2, 80]), o($Vq, [2, 81], { 73: [1, 127] }), { 30: [1, 128] }, o($VH, [2, 55]), { 52: [1, 129] }, o($Vq, [2, 74]), o($Vq, [2, 78]), o($Vq, [2, 82]), o($VG, [2, 20]), o($Vq, [2, 42])],
    defaultActions: { 2: [2, 1], 3: [2, 2], 4: [2, 3], 6: [2, 9], 13: [2, 12], 14: [2, 4], 20: [2, 15], 55: [2, 14], 56: [2, 16], 88: [2, 19], 113: [2, 47] },
    parseError: function parseError(str, hash) {
      if (hash.recoverable) {
        this.trace(str);
      } else {
        var error = new Error(str);
        error.hash = hash;
        throw error;
      }
    },
    parse: function parse(input) {
      var self = this, stack = [0], tstack = [], vstack = [null], lstack = [], table = this.table, yytext = "", yylineno = 0, yyleng = 0, TERROR = 2, EOF = 1;
      var args = lstack.slice.call(arguments, 1);
      var lexer2 = Object.create(this.lexer);
      var sharedState = { yy: {} };
      for (var k in this.yy) {
        if (Object.prototype.hasOwnProperty.call(this.yy, k)) {
          sharedState.yy[k] = this.yy[k];
        }
      }
      lexer2.setInput(input, sharedState.yy);
      sharedState.yy.lexer = lexer2;
      sharedState.yy.parser = this;
      if (typeof lexer2.yylloc == "undefined") {
        lexer2.yylloc = {};
      }
      var yyloc = lexer2.yylloc;
      lstack.push(yyloc);
      var ranges = lexer2.options && lexer2.options.ranges;
      if (typeof sharedState.yy.parseError === "function") {
        this.parseError = sharedState.yy.parseError;
      } else {
        this.parseError = Object.getPrototypeOf(this).parseError;
      }
      function lex() {
        var token;
        token = tstack.pop() || lexer2.lex() || EOF;
        if (typeof token !== "number") {
          if (token instanceof Array) {
            tstack = token;
            token = tstack.pop();
          }
          token = self.symbols_[token] || token;
        }
        return token;
      }
      var symbol, state, action, r, yyval = {}, p, len, newState, expected;
      while (true) {
        state = stack[stack.length - 1];
        if (this.defaultActions[state]) {
          action = this.defaultActions[state];
        } else {
          if (symbol === null || typeof symbol == "undefined") {
            symbol = lex();
          }
          action = table[state] && table[state][symbol];
        }
        if (typeof action === "undefined" || !action.length || !action[0]) {
          var errStr = "";
          expected = [];
          for (p in table[state]) {
            if (this.terminals_[p] && p > TERROR) {
              expected.push("'" + this.terminals_[p] + "'");
            }
          }
          if (lexer2.showPosition) {
            errStr = "Parse error on line " + (yylineno + 1) + ":\n" + lexer2.showPosition() + "\nExpecting " + expected.join(", ") + ", got '" + (this.terminals_[symbol] || symbol) + "'";
          } else {
            errStr = "Parse error on line " + (yylineno + 1) + ": Unexpected " + (symbol == EOF ? "end of input" : "'" + (this.terminals_[symbol] || symbol) + "'");
          }
          this.parseError(errStr, {
            text: lexer2.match,
            token: this.terminals_[symbol] || symbol,
            line: lexer2.yylineno,
            loc: yyloc,
            expected
          });
        }
        if (action[0] instanceof Array && action.length > 1) {
          throw new Error("Parse Error: multiple actions possible at state: " + state + ", token: " + symbol);
        }
        switch (action[0]) {
          case 1:
            stack.push(symbol);
            vstack.push(lexer2.yytext);
            lstack.push(lexer2.yylloc);
            stack.push(action[1]);
            symbol = null;
            {
              yyleng = lexer2.yyleng;
              yytext = lexer2.yytext;
              yylineno = lexer2.yylineno;
              yyloc = lexer2.yylloc;
            }
            break;
          case 2:
            len = this.productions_[action[1]][1];
            yyval.$ = vstack[vstack.length - len];
            yyval._$ = {
              first_line: lstack[lstack.length - (len || 1)].first_line,
              last_line: lstack[lstack.length - 1].last_line,
              first_column: lstack[lstack.length - (len || 1)].first_column,
              last_column: lstack[lstack.length - 1].last_column
            };
            if (ranges) {
              yyval._$.range = [
                lstack[lstack.length - (len || 1)].range[0],
                lstack[lstack.length - 1].range[1]
              ];
            }
            r = this.performAction.apply(yyval, [
              yytext,
              yyleng,
              yylineno,
              sharedState.yy,
              action[1],
              vstack,
              lstack
            ].concat(args));
            if (typeof r !== "undefined") {
              return r;
            }
            if (len) {
              stack = stack.slice(0, -1 * len * 2);
              vstack = vstack.slice(0, -1 * len);
              lstack = lstack.slice(0, -1 * len);
            }
            stack.push(this.productions_[action[1]][0]);
            vstack.push(yyval.$);
            lstack.push(yyval._$);
            newState = table[stack[stack.length - 2]][stack[stack.length - 1]];
            stack.push(newState);
            break;
          case 3:
            return true;
        }
      }
      return true;
    }
  };
  var lexer = function() {
    var lexer2 = {
      EOF: 1,
      parseError: function parseError(str, hash) {
        if (this.yy.parser) {
          this.yy.parser.parseError(str, hash);
        } else {
          throw new Error(str);
        }
      },
      // resets the lexer, sets new input
      setInput: function(input, yy) {
        this.yy = yy || this.yy || {};
        this._input = input;
        this._more = this._backtrack = this.done = false;
        this.yylineno = this.yyleng = 0;
        this.yytext = this.matched = this.match = "";
        this.conditionStack = ["INITIAL"];
        this.yylloc = {
          first_line: 1,
          first_column: 0,
          last_line: 1,
          last_column: 0
        };
        if (this.options.ranges) {
          this.yylloc.range = [0, 0];
        }
        this.offset = 0;
        return this;
      },
      // consumes and returns one char from the input
      input: function() {
        var ch = this._input[0];
        this.yytext += ch;
        this.yyleng++;
        this.offset++;
        this.match += ch;
        this.matched += ch;
        var lines = ch.match(/(?:\r\n?|\n).*/g);
        if (lines) {
          this.yylineno++;
          this.yylloc.last_line++;
        } else {
          this.yylloc.last_column++;
        }
        if (this.options.ranges) {
          this.yylloc.range[1]++;
        }
        this._input = this._input.slice(1);
        return ch;
      },
      // unshifts one char (or a string) into the input
      unput: function(ch) {
        var len = ch.length;
        var lines = ch.split(/(?:\r\n?|\n)/g);
        this._input = ch + this._input;
        this.yytext = this.yytext.substr(0, this.yytext.length - len);
        this.offset -= len;
        var oldLines = this.match.split(/(?:\r\n?|\n)/g);
        this.match = this.match.substr(0, this.match.length - 1);
        this.matched = this.matched.substr(0, this.matched.length - 1);
        if (lines.length - 1) {
          this.yylineno -= lines.length - 1;
        }
        var r = this.yylloc.range;
        this.yylloc = {
          first_line: this.yylloc.first_line,
          last_line: this.yylineno + 1,
          first_column: this.yylloc.first_column,
          last_column: lines ? (lines.length === oldLines.length ? this.yylloc.first_column : 0) + oldLines[oldLines.length - lines.length].length - lines[0].length : this.yylloc.first_column - len
        };
        if (this.options.ranges) {
          this.yylloc.range = [r[0], r[0] + this.yyleng - len];
        }
        this.yyleng = this.yytext.length;
        return this;
      },
      // When called from action, caches matched text and appends it on next action
      more: function() {
        this._more = true;
        return this;
      },
      // When called from action, signals the lexer that this rule fails to match the input, so the next matching rule (regex) should be tested instead.
      reject: function() {
        if (this.options.backtrack_lexer) {
          this._backtrack = true;
        } else {
          return this.parseError("Lexical error on line " + (this.yylineno + 1) + ". You can only invoke reject() in the lexer when the lexer is of the backtracking persuasion (options.backtrack_lexer = true).\n" + this.showPosition(), {
            text: "",
            token: null,
            line: this.yylineno
          });
        }
        return this;
      },
      // retain first n characters of the match
      less: function(n) {
        this.unput(this.match.slice(n));
      },
      // displays already matched input, i.e. for error messages
      pastInput: function() {
        var past = this.matched.substr(0, this.matched.length - this.match.length);
        return (past.length > 20 ? "..." : "") + past.substr(-20).replace(/\n/g, "");
      },
      // displays upcoming input, i.e. for error messages
      upcomingInput: function() {
        var next = this.match;
        if (next.length < 20) {
          next += this._input.substr(0, 20 - next.length);
        }
        return (next.substr(0, 20) + (next.length > 20 ? "..." : "")).replace(/\n/g, "");
      },
      // displays the character position where the lexing error occurred, i.e. for error messages
      showPosition: function() {
        var pre = this.pastInput();
        var c = new Array(pre.length + 1).join("-");
        return pre + this.upcomingInput() + "\n" + c + "^";
      },
      // test the lexed token: return FALSE when not a match, otherwise return token
      test_match: function(match, indexed_rule) {
        var token, lines, backup;
        if (this.options.backtrack_lexer) {
          backup = {
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
          };
          if (this.options.ranges) {
            backup.yylloc.range = this.yylloc.range.slice(0);
          }
        }
        lines = match[0].match(/(?:\r\n?|\n).*/g);
        if (lines) {
          this.yylineno += lines.length;
        }
        this.yylloc = {
          first_line: this.yylloc.last_line,
          last_line: this.yylineno + 1,
          first_column: this.yylloc.last_column,
          last_column: lines ? lines[lines.length - 1].length - lines[lines.length - 1].match(/\r?\n?/)[0].length : this.yylloc.last_column + match[0].length
        };
        this.yytext += match[0];
        this.match += match[0];
        this.matches = match;
        this.yyleng = this.yytext.length;
        if (this.options.ranges) {
          this.yylloc.range = [this.offset, this.offset += this.yyleng];
        }
        this._more = false;
        this._backtrack = false;
        this._input = this._input.slice(match[0].length);
        this.matched += match[0];
        token = this.performAction.call(this, this.yy, this, indexed_rule, this.conditionStack[this.conditionStack.length - 1]);
        if (this.done && this._input) {
          this.done = false;
        }
        if (token) {
          return token;
        } else if (this._backtrack) {
          for (var k in backup) {
            this[k] = backup[k];
          }
          return false;
        }
        return false;
      },
      // return next match in input
      next: function() {
        if (this.done) {
          return this.EOF;
        }
        if (!this._input) {
          this.done = true;
        }
        var token, match, tempMatch, index;
        if (!this._more) {
          this.yytext = "";
          this.match = "";
        }
        var rules = this._currentRules();
        for (var i = 0; i < rules.length; i++) {
          tempMatch = this._input.match(this.rules[rules[i]]);
          if (tempMatch && (!match || tempMatch[0].length > match[0].length)) {
            match = tempMatch;
            index = i;
            if (this.options.backtrack_lexer) {
              token = this.test_match(tempMatch, rules[i]);
              if (token !== false) {
                return token;
              } else if (this._backtrack) {
                match = false;
                continue;
              } else {
                return false;
              }
            } else if (!this.options.flex) {
              break;
            }
          }
        }
        if (match) {
          token = this.test_match(match, rules[index]);
          if (token !== false) {
            return token;
          }
          return false;
        }
        if (this._input === "") {
          return this.EOF;
        } else {
          return this.parseError("Lexical error on line " + (this.yylineno + 1) + ". Unrecognized text.\n" + this.showPosition(), {
            text: "",
            token: null,
            line: this.yylineno
          });
        }
      },
      // return next match that has a token
      lex: function lex() {
        var r = this.next();
        if (r) {
          return r;
        } else {
          return this.lex();
        }
      },
      // activates a new lexer condition state (pushes the new lexer condition state onto the condition stack)
      begin: function begin(condition) {
        this.conditionStack.push(condition);
      },
      // pop the previously active lexer condition state off the condition stack
      popState: function popState() {
        var n = this.conditionStack.length - 1;
        if (n > 0) {
          return this.conditionStack.pop();
        } else {
          return this.conditionStack[0];
        }
      },
      // produce the lexer rule set which is active for the currently active lexer condition state
      _currentRules: function _currentRules() {
        if (this.conditionStack.length && this.conditionStack[this.conditionStack.length - 1]) {
          return this.conditions[this.conditionStack[this.conditionStack.length - 1]].rules;
        } else {
          return this.conditions["INITIAL"].rules;
        }
      },
      // return the currently active lexer condition state; when an index argument is provided it produces the N-th previous condition state, if available
      topState: function topState(n) {
        n = this.conditionStack.length - 1 - Math.abs(n || 0);
        if (n >= 0) {
          return this.conditionStack[n];
        } else {
          return "INITIAL";
        }
      },
      // alias for begin(condition)
      pushState: function pushState(condition) {
        this.begin(condition);
      },
      // return the number of states currently on the stack
      stateStackSize: function stateStackSize() {
        return this.conditionStack.length;
      },
      options: {},
      performAction: function anonymous(yy, yy_, $avoiding_name_collisions, YY_START) {
        switch ($avoiding_name_collisions) {
          case 0:
            this.begin("open_directive");
            return 19;
          case 1:
            return 8;
          case 2:
            return 9;
          case 3:
            return 10;
          case 4:
            return 11;
          case 5:
            this.begin("type_directive");
            return 20;
          case 6:
            this.popState();
            this.begin("arg_directive");
            return 17;
          case 7:
            this.popState();
            this.popState();
            return 22;
          case 8:
            return 21;
          case 9:
            break;
          case 10:
            break;
          case 11:
            this.begin("acc_title");
            return 43;
          case 12:
            this.popState();
            return "acc_title_value";
          case 13:
            this.begin("acc_descr");
            return 45;
          case 14:
            this.popState();
            return "acc_descr_value";
          case 15:
            this.begin("acc_descr_multiline");
            break;
          case 16:
            this.popState();
            break;
          case 17:
            return "acc_descr_multiline_value";
          case 18:
            return 16;
          case 19:
            break;
          case 20:
            return 23;
          case 21:
            return 23;
          case 22:
            this.begin("struct");
            return 50;
          case 23:
            return "EDGE_STATE";
          case 24:
            return "EOF_IN_STRUCT";
          case 25:
            return "OPEN_IN_STRUCT";
          case 26:
            this.popState();
            return 52;
          case 27:
            break;
          case 28:
            return "MEMBER";
          case 29:
            return 53;
          case 30:
            return 78;
          case 31:
            return 71;
          case 32:
            return 72;
          case 33:
            return 74;
          case 34:
            return 59;
          case 35:
            return 61;
          case 36:
            return 54;
          case 37:
            return 55;
          case 38:
            this.begin("generic");
            break;
          case 39:
            this.popState();
            break;
          case 40:
            return "GENERICTYPE";
          case 41:
            this.begin("string");
            break;
          case 42:
            this.popState();
            break;
          case 43:
            return "STR";
          case 44:
            this.begin("bqstring");
            break;
          case 45:
            this.popState();
            break;
          case 46:
            return "BQUOTE_STR";
          case 47:
            this.begin("href");
            break;
          case 48:
            this.popState();
            break;
          case 49:
            return 77;
          case 50:
            this.begin("callback_name");
            break;
          case 51:
            this.popState();
            break;
          case 52:
            this.popState();
            this.begin("callback_args");
            break;
          case 53:
            return 75;
          case 54:
            this.popState();
            break;
          case 55:
            return 76;
          case 56:
            return 73;
          case 57:
            return 73;
          case 58:
            return 73;
          case 59:
            return 73;
          case 60:
            return 65;
          case 61:
            return 65;
          case 62:
            return 67;
          case 63:
            return 67;
          case 64:
            return 66;
          case 65:
            return 64;
          case 66:
            return 68;
          case 67:
            return 69;
          case 68:
            return 70;
          case 69:
            return 36;
          case 70:
            return 49;
          case 71:
            return 90;
          case 72:
            return "DOT";
          case 73:
            return "PLUS";
          case 74:
            return 87;
          case 75:
            return "EQUALS";
          case 76:
            return "EQUALS";
          case 77:
            return 94;
          case 78:
            return 28;
          case 79:
            return 30;
          case 80:
            return "PUNCTUATION";
          case 81:
            return 93;
          case 82:
            return 92;
          case 83:
            return 89;
          case 84:
            return 25;
        }
      },
      rules: [/^(?:%%\{)/, /^(?:.*direction\s+TB[^\n]*)/, /^(?:.*direction\s+BT[^\n]*)/, /^(?:.*direction\s+RL[^\n]*)/, /^(?:.*direction\s+LR[^\n]*)/, /^(?:((?:(?!\}%%)[^:.])*))/, /^(?::)/, /^(?:\}%%)/, /^(?:((?:(?!\}%%).|\n)*))/, /^(?:%%(?!\{)*[^\n]*(\r?\n?)+)/, /^(?:%%[^\n]*(\r?\n)*)/, /^(?:accTitle\s*:\s*)/, /^(?:(?!\n||)*[^\n]*)/, /^(?:accDescr\s*:\s*)/, /^(?:(?!\n||)*[^\n]*)/, /^(?:accDescr\s*\{\s*)/, /^(?:[\}])/, /^(?:[^\}]*)/, /^(?:\s*(\r?\n)+)/, /^(?:\s+)/, /^(?:classDiagram-v2\b)/, /^(?:classDiagram\b)/, /^(?:[{])/, /^(?:\[\*\])/, /^(?:$)/, /^(?:[{])/, /^(?:[}])/, /^(?:[\n])/, /^(?:[^{}\n]*)/, /^(?:class\b)/, /^(?:cssClass\b)/, /^(?:callback\b)/, /^(?:link\b)/, /^(?:click\b)/, /^(?:note for\b)/, /^(?:note\b)/, /^(?:<<)/, /^(?:>>)/, /^(?:[~])/, /^(?:[~])/, /^(?:[^~]*)/, /^(?:["])/, /^(?:["])/, /^(?:[^"]*)/, /^(?:[`])/, /^(?:[`])/, /^(?:[^`]+)/, /^(?:href[\s]+["])/, /^(?:["])/, /^(?:[^"]*)/, /^(?:call[\s]+)/, /^(?:\([\s]*\))/, /^(?:\()/, /^(?:[^(]*)/, /^(?:\))/, /^(?:[^)]*)/, /^(?:_self\b)/, /^(?:_blank\b)/, /^(?:_parent\b)/, /^(?:_top\b)/, /^(?:\s*<\|)/, /^(?:\s*\|>)/, /^(?:\s*>)/, /^(?:\s*<)/, /^(?:\s*\*)/, /^(?:\s*o\b)/, /^(?:\s*\(\))/, /^(?:--)/, /^(?:\.\.)/, /^(?::{1}[^:\n;]+)/, /^(?::{3})/, /^(?:-)/, /^(?:\.)/, /^(?:\+)/, /^(?:%)/, /^(?:=)/, /^(?:=)/, /^(?:\w+)/, /^(?:\[)/, /^(?:\])/, /^(?:[!"#$%&'*+,-.`?\\/])/, /^(?:[0-9]+)/, /^(?:[\u00AA\u00B5\u00BA\u00C0-\u00D6\u00D8-\u00F6]|[\u00F8-\u02C1\u02C6-\u02D1\u02E0-\u02E4\u02EC\u02EE\u0370-\u0374\u0376\u0377]|[\u037A-\u037D\u0386\u0388-\u038A\u038C\u038E-\u03A1\u03A3-\u03F5]|[\u03F7-\u0481\u048A-\u0527\u0531-\u0556\u0559\u0561-\u0587\u05D0-\u05EA]|[\u05F0-\u05F2\u0620-\u064A\u066E\u066F\u0671-\u06D3\u06D5\u06E5\u06E6\u06EE]|[\u06EF\u06FA-\u06FC\u06FF\u0710\u0712-\u072F\u074D-\u07A5\u07B1\u07CA-\u07EA]|[\u07F4\u07F5\u07FA\u0800-\u0815\u081A\u0824\u0828\u0840-\u0858\u08A0]|[\u08A2-\u08AC\u0904-\u0939\u093D\u0950\u0958-\u0961\u0971-\u0977]|[\u0979-\u097F\u0985-\u098C\u098F\u0990\u0993-\u09A8\u09AA-\u09B0\u09B2]|[\u09B6-\u09B9\u09BD\u09CE\u09DC\u09DD\u09DF-\u09E1\u09F0\u09F1\u0A05-\u0A0A]|[\u0A0F\u0A10\u0A13-\u0A28\u0A2A-\u0A30\u0A32\u0A33\u0A35\u0A36\u0A38\u0A39]|[\u0A59-\u0A5C\u0A5E\u0A72-\u0A74\u0A85-\u0A8D\u0A8F-\u0A91\u0A93-\u0AA8]|[\u0AAA-\u0AB0\u0AB2\u0AB3\u0AB5-\u0AB9\u0ABD\u0AD0\u0AE0\u0AE1\u0B05-\u0B0C]|[\u0B0F\u0B10\u0B13-\u0B28\u0B2A-\u0B30\u0B32\u0B33\u0B35-\u0B39\u0B3D\u0B5C]|[\u0B5D\u0B5F-\u0B61\u0B71\u0B83\u0B85-\u0B8A\u0B8E-\u0B90\u0B92-\u0B95\u0B99]|[\u0B9A\u0B9C\u0B9E\u0B9F\u0BA3\u0BA4\u0BA8-\u0BAA\u0BAE-\u0BB9\u0BD0]|[\u0C05-\u0C0C\u0C0E-\u0C10\u0C12-\u0C28\u0C2A-\u0C33\u0C35-\u0C39\u0C3D]|[\u0C58\u0C59\u0C60\u0C61\u0C85-\u0C8C\u0C8E-\u0C90\u0C92-\u0CA8\u0CAA-\u0CB3]|[\u0CB5-\u0CB9\u0CBD\u0CDE\u0CE0\u0CE1\u0CF1\u0CF2\u0D05-\u0D0C\u0D0E-\u0D10]|[\u0D12-\u0D3A\u0D3D\u0D4E\u0D60\u0D61\u0D7A-\u0D7F\u0D85-\u0D96\u0D9A-\u0DB1]|[\u0DB3-\u0DBB\u0DBD\u0DC0-\u0DC6\u0E01-\u0E30\u0E32\u0E33\u0E40-\u0E46\u0E81]|[\u0E82\u0E84\u0E87\u0E88\u0E8A\u0E8D\u0E94-\u0E97\u0E99-\u0E9F\u0EA1-\u0EA3]|[\u0EA5\u0EA7\u0EAA\u0EAB\u0EAD-\u0EB0\u0EB2\u0EB3\u0EBD\u0EC0-\u0EC4\u0EC6]|[\u0EDC-\u0EDF\u0F00\u0F40-\u0F47\u0F49-\u0F6C\u0F88-\u0F8C\u1000-\u102A]|[\u103F\u1050-\u1055\u105A-\u105D\u1061\u1065\u1066\u106E-\u1070\u1075-\u1081]|[\u108E\u10A0-\u10C5\u10C7\u10CD\u10D0-\u10FA\u10FC-\u1248\u124A-\u124D]|[\u1250-\u1256\u1258\u125A-\u125D\u1260-\u1288\u128A-\u128D\u1290-\u12B0]|[\u12B2-\u12B5\u12B8-\u12BE\u12C0\u12C2-\u12C5\u12C8-\u12D6\u12D8-\u1310]|[\u1312-\u1315\u1318-\u135A\u1380-\u138F\u13A0-\u13F4\u1401-\u166C]|[\u166F-\u167F\u1681-\u169A\u16A0-\u16EA\u1700-\u170C\u170E-\u1711]|[\u1720-\u1731\u1740-\u1751\u1760-\u176C\u176E-\u1770\u1780-\u17B3\u17D7]|[\u17DC\u1820-\u1877\u1880-\u18A8\u18AA\u18B0-\u18F5\u1900-\u191C]|[\u1950-\u196D\u1970-\u1974\u1980-\u19AB\u19C1-\u19C7\u1A00-\u1A16]|[\u1A20-\u1A54\u1AA7\u1B05-\u1B33\u1B45-\u1B4B\u1B83-\u1BA0\u1BAE\u1BAF]|[\u1BBA-\u1BE5\u1C00-\u1C23\u1C4D-\u1C4F\u1C5A-\u1C7D\u1CE9-\u1CEC]|[\u1CEE-\u1CF1\u1CF5\u1CF6\u1D00-\u1DBF\u1E00-\u1F15\u1F18-\u1F1D]|[\u1F20-\u1F45\u1F48-\u1F4D\u1F50-\u1F57\u1F59\u1F5B\u1F5D\u1F5F-\u1F7D]|[\u1F80-\u1FB4\u1FB6-\u1FBC\u1FBE\u1FC2-\u1FC4\u1FC6-\u1FCC\u1FD0-\u1FD3]|[\u1FD6-\u1FDB\u1FE0-\u1FEC\u1FF2-\u1FF4\u1FF6-\u1FFC\u2071\u207F]|[\u2090-\u209C\u2102\u2107\u210A-\u2113\u2115\u2119-\u211D\u2124\u2126\u2128]|[\u212A-\u212D\u212F-\u2139\u213C-\u213F\u2145-\u2149\u214E\u2183\u2184]|[\u2C00-\u2C2E\u2C30-\u2C5E\u2C60-\u2CE4\u2CEB-\u2CEE\u2CF2\u2CF3]|[\u2D00-\u2D25\u2D27\u2D2D\u2D30-\u2D67\u2D6F\u2D80-\u2D96\u2DA0-\u2DA6]|[\u2DA8-\u2DAE\u2DB0-\u2DB6\u2DB8-\u2DBE\u2DC0-\u2DC6\u2DC8-\u2DCE]|[\u2DD0-\u2DD6\u2DD8-\u2DDE\u2E2F\u3005\u3006\u3031-\u3035\u303B\u303C]|[\u3041-\u3096\u309D-\u309F\u30A1-\u30FA\u30FC-\u30FF\u3105-\u312D]|[\u3131-\u318E\u31A0-\u31BA\u31F0-\u31FF\u3400-\u4DB5\u4E00-\u9FCC]|[\uA000-\uA48C\uA4D0-\uA4FD\uA500-\uA60C\uA610-\uA61F\uA62A\uA62B]|[\uA640-\uA66E\uA67F-\uA697\uA6A0-\uA6E5\uA717-\uA71F\uA722-\uA788]|[\uA78B-\uA78E\uA790-\uA793\uA7A0-\uA7AA\uA7F8-\uA801\uA803-\uA805]|[\uA807-\uA80A\uA80C-\uA822\uA840-\uA873\uA882-\uA8B3\uA8F2-\uA8F7\uA8FB]|[\uA90A-\uA925\uA930-\uA946\uA960-\uA97C\uA984-\uA9B2\uA9CF\uAA00-\uAA28]|[\uAA40-\uAA42\uAA44-\uAA4B\uAA60-\uAA76\uAA7A\uAA80-\uAAAF\uAAB1\uAAB5]|[\uAAB6\uAAB9-\uAABD\uAAC0\uAAC2\uAADB-\uAADD\uAAE0-\uAAEA\uAAF2-\uAAF4]|[\uAB01-\uAB06\uAB09-\uAB0E\uAB11-\uAB16\uAB20-\uAB26\uAB28-\uAB2E]|[\uABC0-\uABE2\uAC00-\uD7A3\uD7B0-\uD7C6\uD7CB-\uD7FB\uF900-\uFA6D]|[\uFA70-\uFAD9\uFB00-\uFB06\uFB13-\uFB17\uFB1D\uFB1F-\uFB28\uFB2A-\uFB36]|[\uFB38-\uFB3C\uFB3E\uFB40\uFB41\uFB43\uFB44\uFB46-\uFBB1\uFBD3-\uFD3D]|[\uFD50-\uFD8F\uFD92-\uFDC7\uFDF0-\uFDFB\uFE70-\uFE74\uFE76-\uFEFC]|[\uFF21-\uFF3A\uFF41-\uFF5A\uFF66-\uFFBE\uFFC2-\uFFC7\uFFCA-\uFFCF]|[\uFFD2-\uFFD7\uFFDA-\uFFDC])/, /^(?:\s)/, /^(?:$)/],
      conditions: { "acc_descr_multiline": { "rules": [16, 17], "inclusive": false }, "acc_descr": { "rules": [14], "inclusive": false }, "acc_title": { "rules": [12], "inclusive": false }, "arg_directive": { "rules": [7, 8], "inclusive": false }, "type_directive": { "rules": [6, 7], "inclusive": false }, "open_directive": { "rules": [5], "inclusive": false }, "callback_args": { "rules": [54, 55], "inclusive": false }, "callback_name": { "rules": [51, 52, 53], "inclusive": false }, "href": { "rules": [48, 49], "inclusive": false }, "struct": { "rules": [23, 24, 25, 26, 27, 28], "inclusive": false }, "generic": { "rules": [39, 40], "inclusive": false }, "bqstring": { "rules": [45, 46], "inclusive": false }, "string": { "rules": [42, 43], "inclusive": false }, "INITIAL": { "rules": [0, 1, 2, 3, 4, 9, 10, 11, 13, 15, 18, 19, 20, 21, 22, 23, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 41, 44, 47, 50, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84], "inclusive": true } }
    };
    return lexer2;
  }();
  parser2.lexer = lexer;
  function Parser() {
    this.yy = {};
  }
  Parser.prototype = parser2;
  parser2.Parser = Parser;
  return new Parser();
}();
parser.parser = parser;
const parser$1 = parser;
const MERMAID_DOM_ID_PREFIX = "classId-";
let relations = [];
let classes = {};
let notes = [];
let classCounter = 0;
let functions = [];
const sanitizeText = (txt) => common.sanitizeText(txt, getConfig());
const parseDirective = function(statement, context, type) {
  mermaidAPI.parseDirective(this, statement, context, type);
};
const splitClassNameAndType = function(id) {
  let genericType = "";
  let className = id;
  if (id.indexOf("~") > 0) {
    const split = id.split("~");
    className = sanitizeText(split[0]);
    genericType = sanitizeText(split[1]);
  }
  return { className, type: genericType };
};
const setClassLabel = function(id, label) {
  if (label) {
    label = sanitizeText(label);
  }
  const { className } = splitClassNameAndType(id);
  classes[className].label = label;
};
const addClass = function(id) {
  const classId = splitClassNameAndType(id);
  if (classes[classId.className] !== void 0) {
    return;
  }
  classes[classId.className] = {
    id: classId.className,
    type: classId.type,
    label: classId.className,
    cssClasses: [],
    methods: [],
    members: [],
    annotations: [],
    domId: MERMAID_DOM_ID_PREFIX + classId.className + "-" + classCounter
  };
  classCounter++;
};
const lookUpDomId = function(id) {
  if (id in classes) {
    return classes[id].domId;
  }
  throw new Error("Class not found: " + id);
};
const clear = function() {
  relations = [];
  classes = {};
  notes = [];
  functions = [];
  functions.push(setupToolTips);
  clear$1();
};
const getClass = function(id) {
  return classes[id];
};
const getClasses = function() {
  return classes;
};
const getRelations = function() {
  return relations;
};
const getNotes = function() {
  return notes;
};
const addRelation = function(relation) {
  log.debug("Adding relation: " + JSON.stringify(relation));
  addClass(relation.id1);
  addClass(relation.id2);
  relation.id1 = splitClassNameAndType(relation.id1).className;
  relation.id2 = splitClassNameAndType(relation.id2).className;
  relation.relationTitle1 = common.sanitizeText(
    relation.relationTitle1.trim(),
    getConfig()
  );
  relation.relationTitle2 = common.sanitizeText(
    relation.relationTitle2.trim(),
    getConfig()
  );
  relations.push(relation);
};
const addAnnotation = function(className, annotation) {
  const validatedClassName = splitClassNameAndType(className).className;
  classes[validatedClassName].annotations.push(annotation);
};
const addMember = function(className, member) {
  const validatedClassName = splitClassNameAndType(className).className;
  const theClass = classes[validatedClassName];
  if (typeof member === "string") {
    const memberString = member.trim();
    if (memberString.startsWith("<<") && memberString.endsWith(">>")) {
      theClass.annotations.push(sanitizeText(memberString.substring(2, memberString.length - 2)));
    } else if (memberString.indexOf(")") > 0) {
      theClass.methods.push(sanitizeText(memberString));
    } else if (memberString) {
      theClass.members.push(sanitizeText(memberString));
    }
  }
};
const addMembers = function(className, members) {
  if (Array.isArray(members)) {
    members.reverse();
    members.forEach((member) => addMember(className, member));
  }
};
const addNote = function(text, className) {
  const note = {
    id: `note${notes.length}`,
    class: className,
    text
  };
  notes.push(note);
};
const cleanupLabel = function(label) {
  if (label.startsWith(":")) {
    label = label.substring(1);
  }
  return sanitizeText(label.trim());
};
const setCssClass = function(ids, className) {
  ids.split(",").forEach(function(_id) {
    let id = _id;
    if (_id[0].match(/\d/)) {
      id = MERMAID_DOM_ID_PREFIX + id;
    }
    if (classes[id] !== void 0) {
      classes[id].cssClasses.push(className);
    }
  });
};
const setTooltip = function(ids, tooltip) {
  ids.split(",").forEach(function(id) {
    if (tooltip !== void 0) {
      classes[id].tooltip = sanitizeText(tooltip);
    }
  });
};
const getTooltip = function(id) {
  return classes[id].tooltip;
};
const setLink = function(ids, linkStr, target) {
  const config = getConfig();
  ids.split(",").forEach(function(_id) {
    let id = _id;
    if (_id[0].match(/\d/)) {
      id = MERMAID_DOM_ID_PREFIX + id;
    }
    if (classes[id] !== void 0) {
      classes[id].link = utils.formatUrl(linkStr, config);
      if (config.securityLevel === "sandbox") {
        classes[id].linkTarget = "_top";
      } else if (typeof target === "string") {
        classes[id].linkTarget = sanitizeText(target);
      } else {
        classes[id].linkTarget = "_blank";
      }
    }
  });
  setCssClass(ids, "clickable");
};
const setClickEvent = function(ids, functionName, functionArgs) {
  ids.split(",").forEach(function(id) {
    setClickFunc(id, functionName, functionArgs);
    classes[id].haveCallback = true;
  });
  setCssClass(ids, "clickable");
};
const setClickFunc = function(domId, functionName, functionArgs) {
  const config = getConfig();
  if (config.securityLevel !== "loose") {
    return;
  }
  if (functionName === void 0) {
    return;
  }
  const id = domId;
  if (classes[id] !== void 0) {
    const elemId = lookUpDomId(id);
    let argList = [];
    if (typeof functionArgs === "string") {
      argList = functionArgs.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);
      for (let i = 0; i < argList.length; i++) {
        let item = argList[i].trim();
        if (item.charAt(0) === '"' && item.charAt(item.length - 1) === '"') {
          item = item.substr(1, item.length - 2);
        }
        argList[i] = item;
      }
    }
    if (argList.length === 0) {
      argList.push(elemId);
    }
    functions.push(function() {
      const elem = document.querySelector(`[id="${elemId}"]`);
      if (elem !== null) {
        elem.addEventListener(
          "click",
          function() {
            utils.runFunc(functionName, ...argList);
          },
          false
        );
      }
    });
  }
};
const bindFunctions = function(element) {
  functions.forEach(function(fun) {
    fun(element);
  });
};
const lineType = {
  LINE: 0,
  DOTTED_LINE: 1
};
const relationType = {
  AGGREGATION: 0,
  EXTENSION: 1,
  COMPOSITION: 2,
  DEPENDENCY: 3,
  LOLLIPOP: 4
};
const setupToolTips = function(element) {
  let tooltipElem = select(".mermaidTooltip");
  if ((tooltipElem._groups || tooltipElem)[0][0] === null) {
    tooltipElem = select("body").append("div").attr("class", "mermaidTooltip").style("opacity", 0);
  }
  const svg = select(element).select("svg");
  const nodes = svg.selectAll("g.node");
  nodes.on("mouseover", function() {
    const el = select(this);
    const title = el.attr("title");
    if (title === null) {
      return;
    }
    const rect = this.getBoundingClientRect();
    tooltipElem.transition().duration(200).style("opacity", ".9");
    tooltipElem.text(el.attr("title")).style("left", window.scrollX + rect.left + (rect.right - rect.left) / 2 + "px").style("top", window.scrollY + rect.top - 14 + document.body.scrollTop + "px");
    tooltipElem.html(tooltipElem.html().replace(/&lt;br\/&gt;/g, "<br/>"));
    el.classed("hover", true);
  }).on("mouseout", function() {
    tooltipElem.transition().duration(500).style("opacity", 0);
    const el = select(this);
    el.classed("hover", false);
  });
};
functions.push(setupToolTips);
let direction = "TB";
const getDirection = () => direction;
const setDirection = (dir) => {
  direction = dir;
};
const db = {
  parseDirective,
  setAccTitle,
  getAccTitle,
  getAccDescription,
  setAccDescription,
  getConfig: () => getConfig().class,
  addClass,
  bindFunctions,
  clear,
  getClass,
  getClasses,
  getNotes,
  addAnnotation,
  addNote,
  getRelations,
  addRelation,
  getDirection,
  setDirection,
  addMember,
  addMembers,
  cleanupLabel,
  lineType,
  relationType,
  setClickEvent,
  setCssClass,
  setLink,
  getTooltip,
  setTooltip,
  lookUpDomId,
  setDiagramTitle,
  getDiagramTitle,
  setClassLabel
};
const getStyles = (options) => `g.classGroup text {
  fill: ${options.nodeBorder};
  fill: ${options.classText};
  stroke: none;
  font-family: ${options.fontFamily};
  font-size: 10px;

  .title {
    font-weight: bolder;
  }

}

.nodeLabel, .edgeLabel {
  color: ${options.classText};
}
.edgeLabel .label rect {
  fill: ${options.mainBkg};
}
.label text {
  fill: ${options.classText};
}
.edgeLabel .label span {
  background: ${options.mainBkg};
}

.classTitle {
  font-weight: bolder;
}
.node rect,
  .node circle,
  .node ellipse,
  .node polygon,
  .node path {
    fill: ${options.mainBkg};
    stroke: ${options.nodeBorder};
    stroke-width: 1px;
  }


.divider {
  stroke: ${options.nodeBorder};
  stroke: 1;
}

g.clickable {
  cursor: pointer;
}

g.classGroup rect {
  fill: ${options.mainBkg};
  stroke: ${options.nodeBorder};
}

g.classGroup line {
  stroke: ${options.nodeBorder};
  stroke-width: 1;
}

.classLabel .box {
  stroke: none;
  stroke-width: 0;
  fill: ${options.mainBkg};
  opacity: 0.5;
}

.classLabel .label {
  fill: ${options.nodeBorder};
  font-size: 10px;
}

.relation {
  stroke: ${options.lineColor};
  stroke-width: 1;
  fill: none;
}

.dashed-line{
  stroke-dasharray: 3;
}

.dotted-line{
  stroke-dasharray: 1 2;
}

#compositionStart, .composition {
  fill: ${options.lineColor} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

#compositionEnd, .composition {
  fill: ${options.lineColor} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

#dependencyStart, .dependency {
  fill: ${options.lineColor} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

#dependencyStart, .dependency {
  fill: ${options.lineColor} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

#extensionStart, .extension {
  fill: ${options.mainBkg} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

#extensionEnd, .extension {
  fill: ${options.mainBkg} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

#aggregationStart, .aggregation {
  fill: ${options.mainBkg} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

#aggregationEnd, .aggregation {
  fill: ${options.mainBkg} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

#lollipopStart, .lollipop {
  fill: ${options.mainBkg} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

#lollipopEnd, .lollipop {
  fill: ${options.mainBkg} !important;
  stroke: ${options.lineColor} !important;
  stroke-width: 1;
}

.edgeTerminals {
  font-size: 11px;
}

.classTitleText {
  text-anchor: middle;
  font-size: 18px;
  fill: ${options.textColor};
}
`;
const styles = getStyles;
export {
  db as d,
  parser$1 as p,
  styles as s
};
//# sourceMappingURL=styles-16907e1b.js.map
