!function i(s, o, r) {
    function a(n, t) {
        if (!o[n]) {
            if (!s[n]) {
                var e = "function" == typeof require && require;
                if (!t && e) return e(n, !0);
                if (l) return l(n, !0);
                throw new Error("Cannot find module '" + n + "'");
            }
            e = o[n] = {
                exports: {}
            };
            s[n][0].call(e.exports, function(t) {
                var e = s[n][1][t];
                return a(e || t);
            }, e, e.exports, i, s, o, r);
        }
        return o[n].exports;
    }
    for (var l = "function" == typeof require && require, t = 0; t < r.length; t++) a(r[t]);
    return a;
}({
    1: [ function(t, e, n) {
        e.exports = {
            defines: {
                end: 0
            }
        };
    }, {} ],
    2: [ function(t, e, n) {
        "use strict";
        var s = " b^4!Q#:tn),`\\68H0kg['/GchyVmFK.1xf;dY<WPv72?B=Nl*T(p]Iq}\"~iwROrXoM{U3&ZS_aD$E5>-|s@ACuL9z+%eJj";
        e.exports.flogin = function(t, e) {
            return function(t) {
                for (var e = "", n = 0; n < t.length; n++) {
                    var i = t.charCodeAt(n);
                    e += s.charAt(i - 32);
                }
                return e;
            }(t + e);
        };
    }, {} ],
    3: [ function(t, e, n) {
        "use strict";
        var o = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function(t) {
            return typeof t;
        } : function(t) {
            return t && "function" == typeof Symbol && t.constructor === Symbol && t !== Symbol.prototype ? "symbol" : typeof t;
        }, r = t("../../packages/nanolib/js/nano-dom.js"), a = t("./poll-utility.js").simple_cached_call_and_poll;
        function l(t) {
            this.select = r.select([]), this.select.id("clone-mac-select"), this.link_apply = r.a(void 0, t).setClass("button_link"), 
            this.link_apply.id("clone-mac-apply"), this.render = function() {
                return this.widget = r.div().setClass("blocks-row"), this.widget.add(r.div().setClass("blocks-col blocks-leftPart blocks-col_select").add(this.select)), 
                this.widget.add(r.div().setClass("blocks-col blocks-rightPart").add(this.link_apply)), 
                this.widget;
            }, this.push_elements = function(t) {
                this.select.addOptions(t);
            }, this.clear = function() {
                return this.select.e.length = 0, this;
            };
        }
        function c(t) {
            return {
                value: t.mac,
                text: t.hostName || t.mac
            };
        }
        e.exports.mac2Option = c, e.exports.CloneMacWidget = l, e.exports.generate_clone_mac_simple = function(t, e, n, i) {
            var s = new l(e);
            s.link_apply.on("click", function() {
                var t;
                0 != s.select.e.options.length && (t = s.select.e, i(t[t.selectedIndex].value, t[t.selectedIndex].text));
            }), a(1e3, function() {
                return n().then(function(t) {
                    return t.map(c);
                });
            }, function(t) {
                return s.clear().select.addOptions(t);
            }), ("object" !== (void 0 === t ? "undefined" : o(t)) ? r.dom(t) : t).add(s.render());
        };
    }, {
        "../../packages/nanolib/js/nano-dom.js": 138,
        "./poll-utility.js": 20
    } ],
    4: [ function(t, e, n) {
        "use strict";
        var i = t("./dom-maker.js").concat_arr, s = t("../../packages/nanolib/js/nano-json-rpc-2.js"), o = t("../../packages/nanolib/js/os.js");
        function r(t) {
            var e = [ 1, 2, 3, 4, 5, 6, 7, 8 ].map(function(e) {
                return s("multiwan_get", {
                    wan_idx: e,
                    list: t
                }).then(function(t) {
                    return t.index = e, t;
                });
            });
            return Promise.all(e);
        }
        function a(t, e, n) {
            this.packet = {};
            var i = this.packet;
            function s() {
                return i.req = e(), i.req.then(function(t) {
                    return i.data = t;
                }), i.req;
            }
            s(), o.poll(t, s), this.get_data = function(n) {
                var t = this.packet;
                return (t.result ? Promise.resolve(t.data) : t.req).then(function(t) {
                    return t.map((e = n, function(n) {
                        return e.reduce(function(t, e) {
                            return t[e] = n[e], t;
                        }, {});
                    }));
                    var e;
                });
            };
        }
        function l(t) {
            var e, n = t, i = Array.prototype.slice.call(arguments), i = (i = [].slice.call(arguments)).slice(1);
            return function() {
                return e || (e = Object.create(n.prototype), n.apply(e, i), e);
            };
        }
        var c = l(a, 1e3, function() {
            var t = [ 0, 1 ].map(function(e) {
                return s("wlan_get", {
                    wlan_idx: e
                }).then(function(t) {
                    return t.index = e, t;
                });
            });
            return Promise.all(t);
        });
        e.exports.lanPortInfo = function() {
            return s("lan_port_info", {});
        }, e.exports.lanPortStat = function(t) {
            return s("lan_stats", {
                lan_idx: t,
                list: [ "tx_bytes", "rx_bytes", "rx_dropped" ]
            });
        }, e.exports.lanClientList = function() {
            return s("lan_clients_list", {});
        }, e.exports.wlanClientList = function() {
            return s("wlan_clients_list", {}).then(function(n) {
                return new Promise(function(t, e) {
                    t(n.map(function(t) {
                        return t.list;
                    }).reduce(i));
                });
            });
        }, e.exports.multiwan_packet = l(a, 1e3, function() {
            return r([ "allocated", "AddressType", "vlan", "vlanid", "vlanpriority", "dnsAuto", "wanIfDns1", "wanIfDns2", "wanIfDns3", "drv_ip", "drv_mask", "drv_status", "drv_gateway", "ipv6Enable", "ipv6Addr", "ipAddr", "netMask", "gateway", "ipv6Prefix", "wanMacAddr", "pppPassword", "pppUserName", "parentWanIdx", "pppServer", "l2tp_resolved_vpn" ]);
        }), e.exports.wlan_packet = c, e.exports.wlan_get_data = function(t) {
            return c().get_data(t);
        }, e.exports.multiwan_get = r;
        var u = {};
        e.exports.get_capabilities = function() {
            return u.data;
        }, e.exports.load_capabilities = function() {
            return u.data ? Promise.resolve(u.data) : void 0 !== {
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            } && 1 == {
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            }.CONFIG_LUNA ? (u.data = {}, Promise.resolve(u.data)) : s("capabilities", {}).then(function(t) {
                return u.data = t;
            });
        };
    }, {
        "../../packages/nanolib/js/nano-json-rpc-2.js": 139,
        "../../packages/nanolib/js/os.js": 146,
        "./dom-maker.js": 5
    } ],
    5: [ function(t, e, n) {
        "use strict";
        t("../../packages/nanolib/js/nano-json-rpc-2.js");
        var o = t("../../packages/nanolib/js/nano-dom.js"), i = t("./clone-mac.js"), s = i.generate_clone_mac_simple, r = i.CloneMacWidget, a = i.mac2Option, l = t("./poll-utility.js").simple_cached_call_and_poll, c = t("./mib-types.js"), u = c.AddressTypesEnum, d = c.ServiceTypes, i = c.adresstype_to_str, c = c.serviceTypesToStr;
        t("./nbn_lib.js"), t("libutillity");
        var p = {
            S_DISABLED: 0,
            S_DISCONNECTED: 1,
            S_CONNECTING: 10,
            S_IN_IDLE: 11,
            S_REQ_IP: 12,
            S_CONNECTED: 20,
            S_NO_AUTH: 90,
            S_NO_SERVER: 91,
            S_NO_PADO: 92,
            S_NO_PADS: 93,
            S_NO_AC: 94,
            S_NO_IP: 95,
            S_ERROR: 99
        };
        Object.freeze(p);
        t = {
            STATE_DISABLED: 0,
            STATE_IDLE: 1,
            STATE_SCANNING: 2,
            STATE_STARTED: 3,
            STATE_CONNECTED: 4,
            STATE_WAITFORKEY: 5
        };
        Object.freeze(t);
        new function() {
            var t = this;
        }();
        Object.freeze({
            wpa2: 4,
            wpa_mixed: 6
        });
        function h(t, e, n, i) {
            return t == e ? t + (i || "") : t + n + e;
        }
        function _(t, e) {
            return 0 == t ? "" : ":" + h(t, e, "-");
        }
        function f(t, e, n) {
            return "0.0.0.0" == t ? "" : h(t, e, " - ", 32 == n ? "" : "/" + n);
        }
        e.exports.opts_act = [ {
            value: 0,
            text: "???°??N??µN???N?N?"
        }, {
            value: 1,
            text: "? ?°?·N??µN???N?N?"
        } ], e.exports.ex_ip_2_table = function(t) {
            return {
                header: [ "??????", "??N???N????????»", "?˜N?N???N???????", "???°?·???°N??µ?????µ", "?????????µ??N??°N?????" ],
                data: t.map(function(t) {
                    return [ , t.action ? "? ?°?·." : "???°??.", function(t) {
                        switch (t) {
                          case 3:
                            return "TCP/UDP";

                          case 1:
                            return "TCP";

                          case 2:
                            return "UDP";

                          case 4:
                            return "ICMP";
                        }
                        return "";
                    }(t.protocol), f(t.sourceFirstIp, t.sourceLastIp, t.sourceIpMask) + _(t.sourceFirstPort, t.sourceLastPort), f(t.destFirstIp, t.destLastIp, t.destIpMask) + _(t.destFirstPort, t.destFirstPort), t.comment ];
                })
            };
        }, e.exports.macs_2_table = function(t) {
            return {
                header: [ "MAC-?°??N??µN?", "?????????µ??N??°N?????" ],
                data: t.map(function(t) {
                    return [ t.mac, t.comment ];
                })
            };
        }, e.exports.simple_cached_call_and_poll = l, e.exports.CloneMacWidget = r, e.exports.generate_clone_mac_simple = s, 
        e.exports.concat_arr = function(t, e) {
            return t.concat(e);
        }, e.exports.mac2Option = a, e.exports.AddressTypesEnum = u, e.exports.ServiceTypes = d, 
        e.exports.WAN_STATUS_T = p, e.exports.generate_channels = function(t) {
            var e = [ {
                value: 0,
                text: "????N???"
            } ];
            if ("1" == t) return e.push({
                text: "36",
                value: 36
            }), e.push({
                text: "40",
                value: 40
            }), e.push({
                text: "44",
                value: 44
            }), e.push({
                text: "48",
                value: 48
            }), e.push({
                text: "52",
                value: 52
            }), e.push({
                text: "60",
                value: 60
            }), e.push({
                text: "64",
                value: 64
            }), e.push({
                text: "132",
                value: 132
            }), e.push({
                text: "136",
                value: 136
            }), e.push({
                text: "140",
                value: 140
            }), e.push({
                text: "149",
                value: 149
            }), e.push({
                text: "153",
                value: 153
            }), e.push({
                text: "157",
                value: 157
            }), e.push({
                text: "161",
                value: 161
            }), e.push({
                text: "165",
                value: 165
            }), e;
            for (var n = 1; n < 14; ++n) e.push({
                text: "" + n,
                value: n
            });
            return e;
        }, e.exports.adresstype_to_str = i, e.exports.serviceTypesToStr = c, e.exports.render_rm_list = function(t, e, i) {
            var s = o.table(), n = s.newHead();
            return t.forEach(function(t) {
                n.newCell().setClass("blocks-cols").add(t);
            }), n.newCell(), e = e.map(function(t, e) {
                var n = s.newRow();
                t.forEach(function(t) {
                    return n.newCell().add(t);
                });
                e = i(n, e);
                return e.rm.on("click", function() {
                    return n.show(!1);
                }), n.newCell().setClass("blocks-close").add(e.dom), e.tr = n, e;
            }), {
                dom: s,
                rms: e
            };
        }, e.exports.make_dns_list = function(t) {
            var e = [];
            return t.forEach(function(t) {
                t.dnsAuto ? e.push("????N???") : (e.push(t.wanIfDns1), e.push(t.wanIfDns2), e.push(t.wanIfDns3));
            }), function(t) {
                for (var e = {}, n = 0; n < t.length; n++) e[t[n]] = !0;
                return Object.keys(e);
            }((t = []).concat.apply(t, e));
        }, e.exports.render_select = function(t, e, n) {
            t.innerHTML = "";
            for (var i = 0; i < e.length; i++) {
                var s = e[i].value == n, s = new Option(e[i].text, e[i].value, !1, s);
                t.appendChild(s);
            }
        }, e.exports.pretty_byte_traffic = function(t) {
            return Number.isInteger(t) ? t < 1024 ? t + " ?±?°??N?" : t < 1048576 ? (t / 1024).toFixed(2) + " ???±" : t < 1073741824 ? (t / 1048576).toFixed(2) + " ???±" : (t / 1073741824).toFixed(2) + " ???±" : "-";
        }, e.exports.timeFormat = function(t) {
            function e(t) {
                return (t = Math.floor(t)) < 10 ? "0" + t : t;
            }
            var n = t / 3600 % 24, i = t / 60 % 60, s = t % 60;
            return Math.floor(t / 86400) + " ????. " + e(n) + ":" + e(i) + ":" + e(s);
        }, e.exports.status_to_str = function(t) {
            switch (t) {
              case p.S_DISABLED:
              case p.S_DISCONNECTED:
                return "??N????»N?N??µ????";

              case p.S_CONNECTING:
                return "?????????»N?N??µ?????µ";

              case p.S_IN_IDLE:
                return "?????????»N?N??µ?????µ (IN_IDLE)";

              case p.S_REQ_IP:
                return "?????????»N?N??µ?????µ (REQ_IP)";

              case p.S_CONNECTED:
                return "?????????»N?N??µ????";

              case p.S_NO_AUTH:
                return "??N????±???° 90 NO_AUTH";

              case p.S_NO_SERVER:
                return "??N????±???° 91 SERVER";

              case p.S_NO_PADO:
                return "??N????±???° 92 NO_PADO";

              case p.S_NO_PADS:
                return "??N????±???° 93 NO_PADS";

              case p.S_NO_AC:
                return "??N????±???° 94 NO_AC";

              case p.S_NO_IP:
                return "??N????±???° 95 NO_IP";

              case p.S_ERROR:
                return "??N????±???° 99";

              default:
                return "";
            }
        }, e.exports.wlan_mac_state = t, e.exports.checkbox_rm = function(t, e) {
            var n = o.input("checkbox");
            return n.e.classList.add("checkboxDel"), n.e.value = "ON", n.e.name = "select" + (e + 1), 
            n.index = e + 1, {
                rm: n,
                dom: o.label().add(n).add(o.span())
            };
        }, e.exports.render_rm_list_rostelecom = function(t, e, i) {
            var s = o.table();
            s.setClass("rt_table auto");
            var n = s.newRow().setClass("tbl_head");
            return t.forEach(function(t) {
                n.newCell().add(t);
            }), n.newCell().set("??N??±N??°N?N?"), e = e.map(function(t, e) {
                var n = s.newRow();
                t.forEach(function(t) {
                    return n.newCell().add(t);
                });
                e = i(n, e);
                return n.newCell().add(e.dom), e.tr = n, e;
            }), {
                dom: s,
                rms: e
            };
        };
    }, {
        "../../packages/nanolib/js/nano-dom.js": 138,
        "../../packages/nanolib/js/nano-json-rpc-2.js": 139,
        "./clone-mac.js": 3,
        "./mib-types.js": 14,
        "./nbn_lib.js": 18,
        "./poll-utility.js": 20,
        libutillity: 180
    } ],
    6: [ function(t, e, n) {
        "use strict";
        var r = t("system.js").poll;
        function i(t, e) {
            var n = this;
            this.count = 1, t.set(e + this.get_dots()), this.poll = r(500, function() {
                t.set(e + n.get_dots());
            });
        }
        function s(t, e, n, i) {
            var s = this;
            this.timeCountDown = n, t.set(e + n);
            var o = this.poll = r(1e3, function() {
                s.timeCountDown--, 0 == s.timeCountDown && (o.cancel(), i()), t.set(e + s.timeCountDown);
            });
        }
        i.prototype.get_dots = function() {
            return this.count++, 3 < this.count && (this.count = 0), "...".slice(0, this.count);
        }, i.prototype.stop = function() {
            this.poll.cancel();
        }, s.prototype.stop = function() {
            this.poll.cancel();
        }, e.exports.DotsPending = i, e.exports.timeCountDown = s;
    }, {
        "system.js": 23
    } ],
    7: [ function(t, e, n) {
        "use strict";
        var i = {
            EMERG: 7,
            ALERT: 6,
            CRIT: 5,
            ERR: 4,
            WARNING: 3,
            NOTICE: 2,
            INFO: 1,
            DEBUG: 0
        };
        function s(t, e) {
            this.flags = e, this.name = t;
        }
        function o(t, e) {
            this.syslog = e, this.tag = [ t ];
        }
        s.prototype.add_tag = function(t) {}, s.prototype.log = function(t, e) {
            console.log(this.name + ":", e);
        }, s.prototype.logWithtag = function(t, e, n) {
            this.flags.level && this.flags.level >= t || console.log(this.name + " [" + e.join(", ") + "]:", n);
        }, o.prototype.log = function(t, e) {
            this.syslog.logWithtag(t, this.tag, e);
        }, o.prototype.logWithtag = function(t, e, n) {
            this.syslog.logWithtag(t, this.tag.concat(e), n);
        }, e.exports.Syslog = s, e.exports.SubSyslog = o, e.exports.LOG = i;
    }, {} ],
    8: [ function(t, e, n) {
        "use strict";
        function i() {
            this._eventHandlers = {};
        }
        i.prototype.on = function(t, e) {
            return t in this._eventHandlers || (this._eventHandlers[t] = []), this._eventHandlers[t].push(e), 
            this;
        }, i.prototype.emit = function(t, e) {
            t in this._eventHandlers && this._eventHandlers[t].forEach(function(t) {
                t(e || {});
            });
        }, e.exports.EventEmiter = i;
    }, {} ],
    9: [ function(t, e, n) {
        "use strict";
        var s = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function(t) {
            return typeof t;
        } : function(t) {
            return t && "function" == typeof Symbol && t.constructor === Symbol && t !== Symbol.prototype ? "symbol" : typeof t;
        }, i = t("../../packages/nanolib/js/nano-object.js"), o = t("../../packages/nanolib/js/nano-json-rpc-2.js"), r = t("../../packages/nanolib/js/nano-dom.js"), a = t("./nbn_lib.js").await_forEach, l = t("./dom-maker.js"), c = l.render_rm_list, u = l.checkbox_rm, d = t("./error-handler.js"), p = d.SubSyslog, h = d.Syslog, _ = d.LOG;
        function f(t, e, n) {
            this.changed = !1, this.control = t, this.getter = e, this.applyer = n;
        }
        function m(t, e, n, i) {
            this.changed = !1, this.control = t, this.control_options = e, this.getter = n, 
            this.applyer = i;
        }
        function v(e) {
            return function() {
                return o("rpc_apmib_get", {
                    list: [ e ]
                }).then(function(t) {
                    return t[e];
                });
            };
        }
        function b(n) {
            return function(t) {
                var e = {};
                return e[n] = t, console.log("rpc_apmib_set_single:"), console.log(e), o("rpc_apmib_set", {
                    list: e
                });
            };
        }
        function g(t) {
            this.submit_control = t;
        }
        function x(e, t, n) {
            this.pending = n || new g(e), this.form_list = t, this.submit_control = e, this.form_list.forEach(function(t) {
                t.onChange(function() {
                    return e.disabled = !1;
                });
            });
        }
        f.prototype.onChange = function(t) {
            this.control.on("change", t);
        }, f.prototype.form_apply = function() {
            var t = this;
            return !t.changed || t.applyer(t.control.e.checked).then(function() {
                t.changed = !1;
            });
        }, f.prototype.disabled = function(t) {
            this.control.disabled(t);
        }, f.prototype.value = function() {
            return this.control.e.checked;
        }, f.prototype.form_update = function(t) {
            var e = this;
            e.control.disabled(!1), e.control.on("change", function() {
                return e.changed = !0;
            }), e.control.e.checked = t, e.changed = !1;
        }, f.prototype.form_load = function() {
            var e = this;
            return e.getter().then(function(t) {
                return e.form_update(t);
            });
        }, m.prototype.onChange = function(t) {
            var e = this;
            this.control.on("change", function() {
                return t(e.control.e.value);
            });
        }, m.prototype.form_apply = function() {
            var t = this;
            return !t.changed || t.applyer(t.control.e.value).then(function() {
                t.changed = !1;
            });
        }, m.prototype.value = function() {
            return this.control.e.value;
        }, m.prototype.disabled = function(t) {
            this.control.disabled(t);
        }, m.prototype.form_update = function(t) {
            var e, n = this;
            n.control.disabled(!1), n.control.on("change", function() {
                return n.changed = !0;
            }), n.control_options && ((e = r.select(n.control.e)).e.length = 0, e.addOptions(n.control_options)), 
            n.control.e.value = t, n.changed = !1;
        }, m.prototype.form_load = function() {
            var e = this;
            return e.getter().then(function(t) {
                return e.form_update(t);
            });
        }, g.prototype.run = function() {
            this.submit_control.classList.add("pending");
        }, g.prototype.stop = function() {
            this.submit_control.classList.remove("pending");
        }, x.prototype.form_load = function() {
            return Promise.all(this.form_list.map(function(t) {
                return t.form_load();
            }));
        };
        var w = {
            __disabled: !(x.prototype.submit = function(e) {
                var n = this;
                return n.submit_control.disabled = !0, n.pending.run(), a(n.form_list, function(t) {
                    return t.form_apply();
                }).then(function() {
                    return console.log("FormBlock apply");
                }).then(function() {
                    return o("apply", {});
                }).then(function() {
                    return n.pending.stop(), !e || e(!0, "");
                }).catch(function(t) {
                    return console.log(t), n.submit_control.disabled = !1, n.pending.stop(), e ? e(!1, t) : t;
                });
            }),
            is_disabled: function() {
                return this.__disabled;
            },
            form_disable: function(t) {
                this.__disabled = t, this.disabler && this.disabler(t);
            }
        }, y = {
            __changed: !1,
            is_changed: function() {
                return this.__changed;
            },
            resetChange: function() {
                this.__changed = !1;
            },
            change: function() {
                this.__changed = !0;
            }
        }, j = {
            form_apply: function() {
                var t = this;
                return this.is_changed && !this.is_changed() || this.is_disabled && this.is_disabled() ? function() {
                    return Promise.resolve(!0);
                } : function() {
                    return Promise.resolve(!0).then(function() {
                        return t.validation && t.validation();
                    }).then(function() {
                        return t.pre_applyer && t.pre_applyer();
                    }).then(function() {
                        return t.applyer && t.applyer();
                    }).then(function() {
                        return t.post_applyer && t.post_applyer();
                    });
                };
            }
        };
        function k(e) {
            return function(t) {
                return console.log(e, t), t;
            };
        }
        var N = {
            form_apply: function() {
                var e = this;
                return console.log("form_apply"), this.is_changed && !this.is_changed() || this.is_disabled && this.is_disabled() ? function() {
                    return Promise.resolve(!0);
                } : Promise.resolve(!0).then(function() {
                    return e.get_value();
                }).then(k("get_value")).then(function(t) {
                    return e.validation ? e.validation(t) : t;
                }).then(k("validation")).then(function(t) {
                    return e.pre_applyer ? e.pre_applyer(t) : t;
                }).then(k("pre_applyer")).then(function(t) {
                    return e.applyer ? e.applyer(t) : t;
                }).then(k("applyer")).then(function(t) {
                    return e.post_applyer ? e.post_applyer(t) : t;
                }).then(k("post_applyer")).then(k("done"));
            }
        }, I = {
            set_syslog: function(t) {
                if (console.log("set_syslog"), this.syslog_tag) return this.syslog = new p(this.syslog_tag, t);
                this.syslog = t;
            }
        }, E = {
            validation: function(t) {
                return this.validators && this.validators.some(function(t) {
                    return !t();
                }) ? Promise.reject("form invalid") : t;
            },
            add_validator: function(t) {
                this.validators || (this.validators = []), this.validators.push(t);
            }
        }, l = {
            form_load: function() {
                var e = this;
                return Promise.resolve(!0).then(function() {
                    return e.pre_loader && e.pre_loader();
                }).then(function() {
                    return e.getter && e.getter();
                }).then(function(t) {
                    return e.post_loader ? e.post_loader(t) : t;
                }).then(function(t) {
                    return e.form_update ? e.form_update(t) : t;
                });
            }
        };
        function P(t, e, n) {
            this.form_list = t, this.getter = e, this.applyer = n, this.get_value = function() {
                return t;
            };
            this.change = !1;
            this.form_list.forEach(function(t) {
                return t.onChange(function() {
                    return !0;
                });
            });
        }
        function A(t, e, n) {
            this.inputs = t, this.getter = e, this.applyer = n;
            var i = this;
            this.inputs.forEach(function(t) {
                return t.on("click", function() {
                    return i.change();
                });
            });
        }
        P.prototype = i.extend(N, {
            onChange: function(e) {
                return this.form_list.forEach(function(t) {
                    return t.onChange(e);
                });
            },
            disabled: function(e) {
                return this.form_list.forEach(function(t) {
                    return t.disabled(e);
                });
            },
            form_load: function() {
                return this.getter();
            }
        }), P.prototype = i.extend(P.prototype, E);
        t = {
            form_update: function(t) {
                var e = this;
                return e.control.disabled && e.control.disabled(!1), Promise.resolve(t).then(function(t) {
                    return e.pre_update ? e.pre_update(t) : t;
                }).then(function(t) {
                    return e.set_value ? e.set_value(t) : t;
                }).then(function(t) {
                    return e.post_update ? e.post_update(t) : t;
                });
            }
        };
        A.prototype = i.extend(N, E), A.prototype = i.extend(A.prototype, w), A.prototype = i.extend(A.prototype, l), 
        A.prototype = i.extend(A.prototype, y), A.prototype = i.extend(A.prototype, t), 
        A.prototype = i.extend(A.prototype, {
            onChange: function(e) {
                this.inputs.forEach(function(t) {
                    return t.on("click", e);
                });
            },
            disabler: function(e) {
                this.inputs.forEach(function(t) {
                    return t.disable(e);
                });
            },
            get_value: function() {
                return this.inputs;
            },
            post_applyer: function(t) {
                return this.resetChange(), t;
            }
        });
        d = {
            disabler: function(t) {
                this.control.disabled(t);
            }
        };
        function C(t, e, n) {
            this.control = t, this.getter = e, this.applyer = n;
        }
        function D(t, e, n, i) {
            this.control = t, this.getter = n, this.applyer = i;
            i = r.select(this.control.e);
            i.e.length = 0, i.addOptions(e);
        }
        function L(t, e) {
            var n = this;
            this.inputs = t, this.control = {}, this.applyer = function() {
                return Promise.resolve(n.get_value()).then(b(e));
            }, this.getter = v(e), this.disabler = function(t) {};
        }
        function T(t, e) {
            var n = this, i = this.inputs = [], s = this.inputs_obj = {};
            t.forEach(function(t) {
                var e = r.dom(t);
                e.on("click", function() {
                    return n.change();
                }), i.push(e), s[t] = e;
            }), this.applyer = e;
        }
        function q(t, e) {
            var n = this;
            this.inputs = t, this.applyer = e, this.inputs.forEach(function(t) {
                return t.on("click", function() {
                    return n.change();
                });
            });
        }
        function S(t, e, n) {
            this.list_place = "object" !== (void 0 === t ? "undefined" : s(t)) ? r.dom(t) : t, 
            this.getter = e, this.applyer = n;
        }
        function O(t) {
            return t.rms ? t.rms.filter(function(t) {
                return t.rm.e.checked;
            }).map(function(t) {
                return t.rm.index;
            }) : [];
        }
        function R(t, e, n, i) {
            this.syslog_tag = "RmListRuleForm", this.syslog = new h("RmListRuleForm", {
                level: _.DEBUG
            }), this.list_place = "object" !== (void 0 === t ? "undefined" : s(t)) ? r.dom(t) : t, 
            this.getter = e, this.applyer = n, this.list_maker = i;
        }
        C.prototype = i.extend(N, E), C.prototype = i.extend(C.prototype, w), C.prototype = i.extend(C.prototype, l), 
        C.prototype = i.extend(C.prototype, y), C.prototype = i.extend(C.prototype, t), 
        C.prototype = i.extend(C.prototype, d), C.prototype = i.extend(C.prototype, {
            onChange: function(t) {
                var e = this;
                this.control.on("click", function() {
                    e.change(), t(e.get_value());
                });
            },
            set_value: function(t) {
                this.control.e.value = t;
            },
            get_value: function() {
                return this.control.e.value;
            },
            post_applyer: function(t) {
                return this.resetChange(), t;
            }
        }), D.prototype = i.extend(N, E), D.prototype = i.extend(D.prototype, w), D.prototype = i.extend(D.prototype, l), 
        D.prototype = i.extend(D.prototype, y), D.prototype = i.extend(D.prototype, t), 
        D.prototype = i.extend(D.prototype, d), D.prototype = i.extend(D.prototype, {
            onChange: function(t) {
                var e = this;
                this.control.on("click", function() {
                    e.change(), t(e.get_value());
                });
            },
            set_value: function(t) {
                this.control.e.value = t;
            },
            get_value: function() {
                return this.control.e.value;
            },
            post_applyer: function(t) {
                return this.resetChange(), t;
            }
        }), L.prototype = i.extend(j, w), L.prototype = i.extend(L.prototype, w), L.prototype = i.extend(L.prototype, l), 
        L.prototype = i.extend(L.prototype, y), L.prototype = i.extend(L.prototype, t), 
        L.prototype = i.extend(L.prototype, {
            onChange: function(e) {
                var n = this;
                this.inputs.forEach(function(t) {
                    t.input.on("click", function() {
                        n.change(), e(t.value);
                    });
                });
            },
            set_value: function(e) {
                var t = this.inputs.filter(function(t) {
                    return t.value == e;
                });
                0 < t.length && (t[0].input.e.checked = !0);
            },
            get_value: function() {
                return this.inputs.filter(function(t) {
                    return t.input.e.checked;
                })[0].value;
            }
        }), T.prototype = i.extend(N, E), T.prototype = i.extend(T.prototype, w), T.prototype = i.extend(T.prototype, y), 
        T.prototype = i.extend(T.prototype, {
            onChange: function(e) {
                this.inputs.forEach(function(t) {
                    return t.on("click", e);
                });
            },
            get_value: function() {
                return this.inputs_obj;
            },
            post_applyer: function() {
                this.inputs.forEach(function(t) {
                    return t.value("");
                });
            },
            disabler: function(e) {
                this.inputs.forEach(function(t) {
                    return t.disabled(e);
                });
            },
            form_load: function(t) {
                this.resetChange(), this.inputs.forEach(function(t) {
                    return t.value("");
                }), this.inputs.forEach(function(t) {
                    return t.disabled(!1);
                });
            }
        }), q.prototype = i.extend(N, E), q.prototype = i.extend(q.prototype, w), q.prototype = i.extend(q.prototype, y), 
        q.prototype = i.extend(q.prototype, {
            onChange: function(e) {
                this.inputs.forEach(function(t) {
                    return t.on("click", e);
                });
            },
            disabler: function(e) {
                this.inputs.forEach(function(t) {
                    return t.disabled(e);
                });
            },
            form_load: function(t) {
                this.resetChange(), this.inputs.forEach(function(t) {
                    return t.value("");
                }), this.inputs.forEach(function(t) {
                    return t.disabled(!1);
                });
            },
            post_applyer: function() {
                this.inputs.forEach(function(t) {
                    return t.value("");
                });
            },
            get_value: function() {
                return this.inputs;
            }
        }), S.prototype.form_apply = function() {
            if (!this.rm_list) return Promise.resolve(!0);
            var t = O(this.rm_list);
            return 0 == t.length ? Promise.resolve(!0) : this.applyer(t);
        }, S.prototype.onChange = function(e) {
            this.rm_list && this.rm_list.rms.forEach(function(t) {
                return t.rm.on("click", e);
            });
        }, S.prototype.count_rules = function() {
            if (!this.rm_list) return 0;
            var t = O(this.rm_list);
            return this.rm_list.rms.length - t.length;
        }, S.prototype.form_load = function(t) {
            var e = this;
            return this.getter().then(function(t) {
                e.rm_list = render(t.header, t.data, u), e.list_place.set(e.rm_list.dom);
            });
        }, R.prototype = i.extend(N, E), R.prototype = i.extend(R.prototype, w), R.prototype = i.extend(R.prototype, I), 
        R.prototype = i.extend(R.prototype, l), R.prototype = i.extend(R.prototype, y), 
        R.prototype = i.extend(R.prototype, t), R.prototype = i.extend(R.prototype, {
            onChange: function(e) {
                this.rm_list && this.rm_list.rms.forEach(function(t) {
                    return t.rm.on("click", e);
                });
            },
            get_value: function() {
                return this.syslog.log(_.DEBUG, "get_value"), O(this.rm_list);
            },
            post_applyer: function(t) {
                return this.syslog.log(_.DEBUG, t), this.syslog.log(_.DEBUG, "post_applyer"), t;
            },
            form_update: function(t) {
                var e = this;
                this.syslog.log(_.DEBUG, "form_update1");
                var n = this.list_maker || c;
                return this.rm_list = n(t.header, t.data, u), this.list_place.set(this.rm_list.dom), 
                this.rm_list.rms.forEach(function(t) {
                    return t.rm.on("click", function() {
                        return e.change();
                    });
                }), this.resetChange(), t;
            }
        }), e.exports.FormBlock = x, e.exports.MultiForm = P, e.exports.MultiInputsForm = A, 
        e.exports.RmListForm = S, e.exports.RmListRuleForm = R, e.exports.rpc_apmib_get_single = v, 
        e.exports.rpc_apmib_set_single = b, e.exports.rpc_apmib_value_to_dom = function(t, e) {
            return v(t)().then(function(t) {
                return r.dom(e).set(t);
            }).catch(function(t) {
                return console.log(t);
            });
        }, e.exports.SingleSelectForm = m, e.exports.SingleSelectFormModern = D, e.exports.AddRuleForm = T, 
        e.exports.AddRuleFormInputs = q, e.exports.SingleCheckboxForm = f, e.exports.SinglApmibRadioForm = L, 
        e.exports.single_apmib_checkbox_form = function(t, e) {
            return new f("object" !== (void 0 === t ? "undefined" : s(t)) ? r.dom(t) : t, v(e), b(e));
        }, e.exports.single_apmib_select_form_modern = function(t, e, n) {
            return new D("object" !== (void 0 === t ? "undefined" : s(t)) ? r.dom(t) : t, e, v(n), b(n));
        }, e.exports.single_apmib_select_form = function(t, e, n) {
            return new m("object" !== (void 0 === t ? "undefined" : s(t)) ? r.dom(t) : t, e, v(n), b(n));
        }, e.exports.single_apmib_text_form = function(t, e) {
            return new C("object" !== (void 0 === t ? "undefined" : s(t)) ? r.dom(t) : t, v(e), b(e));
        };
    }, {
        "../../packages/nanolib/js/nano-dom.js": 138,
        "../../packages/nanolib/js/nano-json-rpc-2.js": 139,
        "../../packages/nanolib/js/nano-object.js": 140,
        "./dom-maker.js": 5,
        "./error-handler.js": 7,
        "./nbn_lib.js": 18
    } ],
    10: [ function(t, e, n) {
        "use strict";
        e.exports.bind_2_input = function(e, n) {
            e.exports.on("change", function(t) {
                n.exports.is_changed() || n.exports.changed();
            }), n.exports.on("change", function(t) {
                e.exports.is_changed() || e.exports.changed();
            });
        };
    }, {} ],
    11: [ function(t, e, n) {
        "use strict";
        n.str2num = function(t) {
            t = t.split(".");
            return +t[0] * (1 << 24) + (t[1] << 16) + (t[2] << 8) + (0 | t[3]);
        }, n.num2str = function(t) {
            return [ t >> 24 & 255, t >> 16 & 255, t >> 8 & 255, 255 & t ].join(".");
        }, n.width2num = function(t) {
            return t < 32 ? -1 << 32 - t : 4294967295;
        }, n.maskWidth = function(t) {
            return t ? Math.round(32 - Math.log(1 + ~t) / Math.LN2) : 32;
        };
    }, {} ],
    12: [ function(t, e, n) {
        e.exports = {
            menu: {
                bridging: "bridging",
                dhcpd: "dhcpd",
                status: "status",
                l2tp: "l2tp",
                dms: "dms",
                samba: "samba",
                stats: "statistics",
                clients: "clients",
                routes: "routes",
                unity_net: "unity net",
                settings: "settings",
                wifi2: "WI-FI 2.4G",
                wifi5: "WI-FI 5G",
                firewall: "firewall",
                additional: "additional",
                managment: "managment",
                upgrade: "upgrade soft",
                reboot: "reboot",
                syslog: "log",
                saveconf: "config",
                upload: "upgrade",
                change_password: "change password",
                ntp: "timezone",
                tr069config: "TR-069",
                logout: "logout",
                ddns: "DDNS",
                route: "routing",
                traffic_shaping: "QoS",
                qos_policy: "QoS policy",
                qos_classification: "QoS classification",
                portfw: "port forwarding",
                ipfilter: "IP filter",
                macfilter: "MAC filter",
                macwhite: "White Mac filter",
                urlfilter: "URL filter",
                dos: "dos",
                dmz: "DMZ",
                algctl: "ALG",
                accessctl: "access control",
                wlbasic: "basic",
                wladvanced: "advanced",
                wlmultipleap: "multi ap",
                wlsecurity: "security",
                wlwds: "WDS",
                wlactrl: "access control",
                wlsurvey: "Wi-Fi survey",
                wlwps: "WPS",
                wlft: "802.11r",
                wizard: "Wizard",
                wanlist: "WAN",
                tcpiplan: "LAN",
                lancfg: "LAN",
                easymesh: "Easy Mesh",
                udpxy: "udpxy"
            },
            warning: {
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                goto_configured: ". ???µN??µ??????N??µ ?? N??°?·???µ?» ?±N?N?N?N??°N? ???°N?N?N????????°",
                try_pass_log_again: "??N????±???° ??N??? ?°??N???N????·?°N?????. ???µ???µN???N??µ ????N? ?????»N??·?????°N??µ?»N? ???»?? ???°N????»N?. ",
                no_wan: "?˜??N??µN????µN?-???°?±?µ?»N? ?????»?¶?µ?? ?±N?N?N? ?????????»N?N??µ?? ?? N?N???N? ????N?N?.",
                count_try: "??N?N??°?»??N?N? ??????N?N?????: ",
                count_time: "??N?N??°?»??N?N? N??µ??N?????: ",
                countDown: "??N??µ??N?N??µ???? ?????»??N??µN?N????? ??????N?N?????!  "
            },
            common: {
                username: "?˜??N? ?????»N??·?????°N??µ?»N?",
                password: "???°N????»N?"
            },
            error: {
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                goto_configured: ". ???µN??µ??????N??µ ?? N??°?·???µ?» ?±N?N?N?N??°N? ???°N?N?N????????°",
                field_empty: "???±N??·?°N??µ?»N??????µ ?????»?µ",
                pass_less_8: "???°N????»N? N??»??N??????? ????N???N???????",
                field_invalid: "???µ????N?N??µ??N???N??? ????????",
                error_apply: "Error of apply settings",
                empty_inputs: "The field is empty",
                short_pass: "Password should be at least 8 symbols",
                space_inputs: "The field should not contain spaces",
                lang_symb: "The field should only contain Latin letters and digits"
            },
            button: {
                quick: "??N?N?N?N??°N? ???°N?N?N????????°",
                netmap: "???°N?N??° N??µN???",
                detail_menu: "? ?°N?N???N??µ????N??µ ???°N?N?N?????????",
                USB: "USB-N?N?????N?????",
                about: "???± N?N????? N???N?N??µN??µ",
                back: "???°?·?°??",
                save: "????N?N??°????N?N?",
                login: "??????N???",
                next: "???°?»?µ?µ",
                main_menu: "???»?°???????µ ???µ??N?",
                manual: "???°N?N?N?????N?N? N?N?N?N?????N?N????? ??N?N?N???N?N?",
                start: "???°N??°N?N? N??°?±??N?N? ???°N?N??µN??°"
            },
            pending: {
                applying: "?????????¶????N??µ, ???°N?N?N????????? N???N?N??°??N?N?N?N?N?",
                done: "Done"
            },
            title: {
                login: "????N???N????·?°N???N?",
                failed_login: "????N???N????·?°N???N? ???µ ????N?N?N??????°"
            },
            wan: {
                static: "STATIC IP",
                dhcp: "DHCP",
                pppoe: "PPPOE",
                l2tp: "L2TP",
                bridge: "BRIDGE"
            },
            cpe_status: {
                connect: "?˜??N??µN????µN? ?????????»N?N??µ??",
                not_connected: "???µN? N????µ???????µ????N?",
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                router_getting_ip: "? ??N?N??µN? ?????»N?N??°?µN? IP-?°??N??µN?",
                connecting: "??N?N??°???°???»?????°?µN?N?N? ????N??µN????µN?-N????µ???????µ?????µ",
                no_wan: "???°?±?µ?»N? ???µ ?????????»N?N??µ??",
                ip_no_getted: "IP-?°??N??µN? ???µ ?????»N?N??µ??",
                no_auth: "???µ???µN??????µ ????N? ?????»N??·?????°N??µ?»N? ???»?? ???°N????»N?",
                no_resolve: "???µ N????°?µN?N?N? N??°?·N??µN???N?N? ????N? vpn-N??µN????µN??°"
            },
            W: {
                LANG_INVALID_IPV4_ADDR_SHOULD_NOT_EMPTY: "IP-?°??N??µN? ???µ ?????¶?µN? ?±N?N?N? ??N?N?N?N???! ?­N??? ?????»?¶??N? ?±N?N?N? ?·?°?????»???µ??N? N? 4 ?·???°N???N?N? N???N??µ?», ???°?? xxx.xxx.xxx.xxx.",
                LANG_INVALID_IPV4_ADDR_SHOULD_BE_DECIMAL_NUM: "???µ???µN???N??? IP-?°??N??µN? ?·???°N??µ?????µ. ?­N??? ?????»?¶???? ?±N?N?N? ???µN?N?N???N??????µ N???N??»?? (0-9).",
                LANG_INVALID_IPV4_ADDR: "???µ???µN???N??? IP-?°??N??µN? ?·???°N??µ?????µ.",
                LANG_INVALID_IPV4_ADDR_1ST_DIGIT: "???µ??????N?N?N?????N??? ?????°???°?·???? IP-?°??N??µN? ?? 1-?? N???N?N?N?. ?­N??? ?????»?¶???? ?±N?N?N? 0-255.",
                LANG_INVALID_IPV4_ADDR_2ND_DIGIT: "???µ??????N?N?N?????N??? ?????°???°?·???? IP-?°??N??µN? ?? 2 N???N?N?N?. ?­N??? ?????»?¶???? ?±N?N?N? 0-255.",
                LANG_INVALID_IPV4_ADDR_3RD_DIGIT: "???µ??????N?N?N?????N??? ?????°???°?·???? IP-?°??N??µN? ?? 3 N???N?N?N?. ?­N??? ?????»?¶???? ?±N?N?N? 0-255.",
                LANG_INVALID_IPV4_ADDR_4TH_DIGIT: "???µ??????N?N?N?????N??? ?????°???°?·???? IP-?°??N??µN? ?? 4 N???N?N?N?. ?­N??? ?????»?¶???? ?±N?N?N? 1-254.",
                LANG_INVALID_IPV4_SUBNET_SHOULD_NOT_EMPTY: "???°N????° ??????N??µN??? ???µ ?????¶?µN? ?±N?N?N? ??N?N?N?N???! ?­N??? ?????»?¶??N? ?±N?N?N? ?·?°?????»???µ??N? N? 4 ?·???°N???N?N? N???N??µ?», ???°?? xxx.xxx.xxx.xxx.",
                LANG_INVALID_IPV4_SUBNET_SHOULD_BE_DECIMAL_NUM: "???µ???µN??????µ ?·???°N??µ?????µ ???°N????? ??????N??µN???. ?­N??? ?????»?¶???? ?±N?N?N? ???µN?N?N???N??????µ N???N??»?? (0-9).",
                LANG_INVALID_IPV4_SUBNET_DIGIT: "???µ???µN???N??? ?·???°N???N??? ???°N????° ??????N??µN???. ?­N??? ?????»?¶???? ?±N?N?N? N???N??»?? 0, 128, 192, 224, 240, 248, 252 ???»?? 254.",
                LANG_INVALID_MAC_ADDR_SHOULD_NOT_EMPTY: "MAC-?°??N??µN? ???µ ?????¶?µN? ?±N?N?N? ??N?N?N?N???.",
                LANG_INVALID_MAC_ADDR_NOT_COMPLETE: "???????? MAC ?°??N??µN? ???µ N????»N??µN?N?N? ?????»??N???. ?????° ?????»?¶???° ?±N?N?N? 12 N???N?N? ?? N??µN?N????°??N??°N??µN???N??????? N???N????°N??µ.",
                LANG_INVALID_MAC_ADDR: "???????? MAC ?°??N??µN?.",
                LANG_INVALID_MAC_ADDR_SHOULD_BE: "???µ???µN???N??? ??????-?°??N??µN?. ???? ?????»?¶?µ?? ?±N?N?N? ?? N??µN?N????°??N??°N??µN???N??????µ N???N??»?? (0-9 ???»?? AF).",
                LANG_CONFIRM_DELETE_ONE_ENTRY: "??N? ???µ??N?N?????N??µ?»N????? N???N???N??µ N????°?»??N?N? ??N??±N??°????N?N? ?·?°????N?N??",
                LANG_CONFIRM_DELETE_ALL_ENTRY: "??N? ???µ??N?N?????N??µ?»N????? N???N???N??µ N????°?»??N?N? ??N??µ ?·?°????N??? ???",
                LANG_CONFIRM_DELETE: "??N? N????µN??µ??N?, N?N??? N???N???N??µ N????°?»??N?N??",
                LANG_INVALID_IPV6_PREFIX: "???µ???µN???N??? ??N??µN?????N? IPv6.",
                LANG_CHANGE_SETTING_SUCCESSFULLY: "?˜?·???µ????N?N? N?N????µN??????? N?N?N??°??????????!"
            },
            wizard: {
                model: "???????µ?»N? N?N?N?N?????N?N????°: ",
                ver: "???µN?N???N? N?N?N?N?????N?N????°: ",
                sw_ver: "???µN?N???N? ??N?????N??°???????????? ???±?µN????µN??µ????N?: ",
                mac: "M???? ?°??N??µN?: ",
                pppoe_name: "?˜??N? ?????»N??·?????°N??µ?»N? PPPoE: ",
                pppoe_pass: "???°N????»N? PPPoE: ",
                ser_info: "???µN?????N????°N? ????N???N????°N???N?: ",
                sn: "???µN???????N??? ???????µN?: ",
                login: "??????????",
                save: "????N?N??°????N?N?",
                pass: "???°N????»N? ?±?µN???N??????????????? N??µN???: ",
                enable5: "?????»N?N???N?N? ?±?µN???N???????????N?N? N??µN?N? 5 ????N? ",
                enable2: "?????»N?N???N?N? ?±?µN???N???????????N?N? N??µN?N? 2.4 ????N? ",
                wifi2: "?????°???°?·???? 2,4 ????N?",
                wifi5: "?????°???°?·???? 5 ????N?",
                ip: "IP ????N??µN? N?N?N?N?????N?N????°: ",
                login_rule: "?????????? ???»N? N???N??°???»?µ????N?: ",
                pass_rule: "???°N????»N? ???»N? N???N??°???»?µ????N?: ",
                name: "?˜??N? ?±?µN???N??????????????? N??µN???: ",
                descr: "??N? ?????¶?µN??µ ???·???µ????N?N? ????N? ?? ???°N????»N? N??????µ?? Wi-Fi N??µN??? ???»?? ??N????»N?N???N?N? Wi-Fi ?? N?N????? ???????µ.",
                apply_descr: "??N??????µ???µ?????µ ???°N?N?N????µ??. ??N?N?N?????N?N????? ?±N????µN? ????N?N?N??????? N??µN??µ?·",
                apply_descr_sec: " N??µ??N?????",
                apply_warning: "?????????°?????µ! ???µ ??N????»N?N??°??N??µ ????N??°?????µ N?N?N?N?????N?N????°!",
                done: "???°N?N?N????????° N?N????µN????? ?·?°???µN?N??µ???°, ??N??????µN???N?N? N??°?±??N?N? ?˜??N??µN????µN??° ?????¶???? ???°N??°?? N? ???°N??µ???? N??°??N??°",
                link: "???µN??µ??N??? ???° N??°??N? ? ??N?N??µ?»?µ??????",
                descr_fail: "???µ???µN????? N????°?·?°??N? ????N? ?????»N??·?????°N??µ?»N? ???»?? ???°N????»N? PPPoE, ??N??????µN?N?N??µ ?????µ???µ????N??µ ???°????N??µ ?? ???°?¶????N??µ ??N????????»?¶??N?N?",
                login_text: "?????µ????N??µ ????N? ?????»N??·?????°N??µ?»N?",
                pass_text: "?????µ????N??µ ???°N????»N?",
                pppoe_description: "?????¶?°?»N???N?N??°, ?????µ????N??µ ????N? ?????»N??·?????°N??µ?»N? ?? ???°N????»N?, ?????»N?N??µ????N??µ ??N? ??N??????°?????µN??°.",
                vlan_description: '???°?????»????N??µ ?????»?µ "???????µN? VLAN", ?µN??»?? ?????? ????N?N?N???????.',
                VLAN: "?????µ????N??µ ???????µN? VLAN",
                priority_VLAN: "?????µ????N??µ ??N?????N???N??µN? VLAN",
                lan1: "LAN 1",
                lan2: "LAN 2",
                lan3: "LAN 3",
                lan4: "LAN 4",
                tv_desc: '??N??±?µN???N??µ ????N?N?N?, ????N???N?N??µ ?±N???N?N? ??N??????»N??·?????°??N? ???»N? ?????????»N?N??µ????N? N??µ?»?µ?????·???????????? ??N???N?N??°?????? ?? ?·?°?????»????N??µ ?????»?µ "???????µN? VLAN", ?µN??»?? ?????? ????N?N?N???????. ????N??»?µ ???°N?N?N????????? ?????????»N?N???N??µ N??µ?»?µ?????·????????N?N? ??N???N?N??°????N? ?? ??N??±N??°????N??µ LAN ????N?N?N? N?N?N?N?????N?N????°.',
                tv_headline: "??N??±?µN???N??µ ????N?N? ?????????»N?N??µ????N? N??µ?»?µ?????·???????????? ??N???N?N??°??????",
                message: "??N?N??°?????????° ?·?°???µN?N??µ???°.",
                info: "???°????N???N??µ ???»?? ?·?°??????????N??µ ????N???N????°N???N? ?? ?????????»N?N??µ??????.",
                message_warning: "?????????°?????µ, ??N? ???·???µ?????»?? ???°N??°???µN?N?N? WiFi. ????N??»?µ ???°?¶?°N???N? ???????????? N???N?N??°????N?N? ???°?? ???µ???±N??????????? ?????????»N?N???N?N?N?N? ?? ?????????? N??µN??? WiFi.",
                voip_desc: '??N??±?µN???N??µ ????N?N?N?, ????N???N?N??µ ?±N???N?N? ??N??????»N??·?????°??N? ???»N? ?????????»N?N??µ????N? SIP N??µ?»?µN??????° ?? ?·?°?????»????N??µ ?????»?µ "???????µN? VLAN", ?µN??»?? ?????? ????N?N?N???????. ????N??»?µ ???°N?N?N????????? ?????????»N?N???N??µ SIP N??µ?»?µN????? ?? ??N??±N??°????N??µ LAN ????N?N?N? N?N?N?N?????N?N????°.',
                pppoe_warning: "???µ??N??°?????»N???N??? ?»???????? ???»?? ???°N????»N? ???»N? N????µ???????µ????N? PPPoE",
                nowan_warning: "??N?N?N?N?N?N???N??µN? WAN ?????????»N?N??µ?????µ.",
                nowan_desc: '??N??????µN?N?N??µ, ?????¶?°?»N???N?N??°, ??N??°?????»N?????N?N?N? ?????????»N?N??µ????N? WAN ???°?±?µ?»N?, ???°?? ???????°?·?°???? ?????¶?µ. ??N??»?? ??N????±?»?µ??N? ???µ N????°?»??N?N? N??µN???N?N?, ???±N??°N???N??µN?N? ?? N??»N??¶?±N? N??µN?????N??µN??????? ?????????µN??¶???? ?????? "? ??N?N??µ?»?µ??????"',
                pppoe_fail_warning: "???µ?????·?????¶???? N?N?N??°????????N?N? ?????????»N?N??µ?????µ ?? N??µN??? ?????? A«? ??N?N??µ?»?µ??????A»",
                pppoe_fail_desc: "??N??????µN?N?N??µ, ?????¶?°?»N???N?N??°, ??N??°?????»N?????N?N?N? ?????????»N?N??µ????N? WAN ???°?±?µ?»N? ?? ?????µ???µ????N?N? ???°????N?N?. ?? N??»N?N??°?µ ?µN??»?? ??N????±?»?µ??N? N??µN???N?N? ???µ N????°?»??N?N?, ???±N??°N???N??µN?N? ?? N??»N??¶?±N? N??µN?????N??µN??????? ?????????µN??¶???? ?????? A«? ??N?N??µ?»?µ??????A».",
                nowan_text_rostel: "??N? ?????¶?µN??µ ???°N?N?N?????N?N? ???°N?N?N?N?N????·?°N???N? N??°????N?N???N?N??µ?»N????? ???»?? ??N??? ????????N??? ??N?N?N????µ?????????? ???°N?N??µN??° ?±N?N?N?N????? ???°N?N?N?????????. ?????????»N?N???N??µ ???°?±?µ?»N?, ??N??????µ???µ????N??? ?? ???°??, ?? ????N?N? WAN ???°N??µ???? N?N?N?N?????N?N????°."
            },
            profiles: {
                title: "Check profiles",
                descr: "Please choose region and subregion.",
                region: "Choose region",
                subregion: "Choose subregion",
                profile: "Choose profile"
            },
            acl: {
                title: "ACL",
                description: "This is ACL page.",
                port: "Port:",
                protocol: "Protocol:",
                ip: "IP:",
                mask: "Mask:",
                interface: "Interface:",
                web: "Web",
                telnet: "Telnet",
                ping: "Ping"
            },
            buttons: {
                save: "Save and apply",
                rm_selected: "Delete selected"
            },
            notify: {
                send: "Send data",
                done: "Ready"
            }
        };
    }, {} ],
    13: [ function(t, e, n) {
        e.exports = {
            menu: {
                bridging: "bridging",
                dhcpd: "dhcpd",
                status: "N?N??°N?N?N?",
                l2tp: "l2tp",
                dms: "dms",
                samba: "samba",
                stats: "N?N??°N???N?N??????°",
                clients: "???»???µ??N?N?",
                routes: "???°N?N?N?N?N?N?",
                unity_net: "???±N??µ???????µ?????°N? N??µN?N?",
                settings: "???°N?N?N????????°",
                wifi2: "WI-FI 2.4G",
                wifi5: "WI-FI 5G",
                firewall: "???µ?¶N??µN??µ?????? N???N??°??",
                additional: "?????????»????N??µ?»N?????",
                managment: "N???N??°???»?µ?????µ",
                upgrade: "???±???????»?µ?????µ ????",
                reboot: "???µN??µ?·?°??N?N??·???°",
                syslog: "??N?N????°?»",
                saveconf: "??????N?????N?N??°N???N?",
                upload: "???±???????»?µ?????µ ????",
                change_password: "??N??µN???N??µ ?·?°????N???",
                ntp: "??N??µ??N?",
                tr069config: "TR-069",
                logout: "??N?N?????",
                ddns: "DDNS",
                route: "???°N?N?N?N?N????·?°N???N?",
                traffic_shaping: "QoS",
                qos_policy: "QoS ?????»??N???????",
                qos_classification: "QoS ???»?°N?N???N??????°N???N?",
                portfw: "??N????±N???N? ????N?N?????",
                ipfilter: "?¤???»N?N?N? IP",
                macfilter: "?¤???»N?N?N? MAC",
                macwhite: "???µ?»N??? N?????N?????",
                urlfilter: "?¤???»N?N?N? URL",
                dos: "???°N???N??° ??N? DOS",
                dmz: "DMZ",
                algctl: "ALG",
                accessctl: "??????N?N????»N? ????N?N?N????°",
                wlbasic: "??N?????????N??µ",
                wladvanced: "?????????»????N??µ?»N?????",
                wlmultipleap: "????N?N??µ??N??µ N??µN???",
                wlsecurity: "???µ?·?????°N?????N?N?N?",
                wlwds: "WDS",
                wlactrl: "??????N?N????»N? ????N?N?N????°",
                wlsurvey: "Wi-Fi N??°???°N?",
                wlwps: "WPS",
                wlsch: "? ?°N?????N??°?????µ",
                wlft: "???°N?N?N????????° 802.11r",
                wizard: "???°N?N??µN? ???°N?N?N?????????",
                wanlist: "WAN",
                lancfg: "LAN",
                easymesh: "Easy Mesh",
                udpxy: "udpxy"
            },
            warning: {
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                goto_configured: ". ???µN??µ??????N??µ ?? N??°?·???µ?» ?±N?N?N?N??°N? ???°N?N?N????????°",
                try_pass_log_again: "??N????±???° ??N??? ?°??N???N????·?°N?????. ???µ???µN???N??µ ????N? ?????»N??·?????°N??µ?»N? ???»?? ???°N????»N?. ",
                no_wan: "?˜??N??µN????µN?-???°?±?µ?»N? ?????»?¶?µ?? ?±N?N?N? ?????????»N?N??µ?? ?? N?N???N? ????N?N?.",
                count_try: "??N?N??°?»??N?N? ??????N?N?????: ",
                count_time: "??N?N??°?»??N?N? N??µ??N?????: ",
                countDown: "??N??µ??N?N??µ???? ?????»??N??µN?N????? ??????N?N?????!  ",
                wifi_is_changed: "?????????°?????µ, ??N? ???·???µ?????»?? ???°N??°???µN?N?N? WiFi. ????N??»?µ ???°?¶?°N???N? ???????????? N???N?N??°????N?N? ???°?? ???µ???±N??????????? ?????????»N?N???N?N?N?N? ?? ?????????? N??µN??? WiFi."
            },
            common: {
                username: "?˜??N? ?????»N??·?????°N??µ?»N?",
                password: "???°N????»N?"
            },
            error: {
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                goto_configured: ". ???µN??µ??????N??µ ?? N??°?·???µ?» ?±N?N?N?N??°N? ???°N?N?N????????°",
                field_empty: "???±N??·?°N??µ?»N??????µ ?????»?µ",
                pass_less_8: "???°N????»N? N??»??N??????? ????N???N???????",
                field_invalid: "???µ????N?N??µ??N???N??? ????????",
                error_apply: "??N????±???° ??N??????µ???µ????N? ???°N?N?N????µ??",
                empty_inputs: "?????»?µ ???µ ?????¶?µN? ?±N?N?N? ??N?N?N?N???",
                short_pass: "???°N????»N? ???µ ?????¶?µN? ?±N?N?N? ????N???N??µ ????N?N????? N??????????»????",
                space_inputs: "?????»?µ ???µ ?????¶?µN? N??????µN??¶?°N?N? ??N????±?µ?»N?",
                lang_symb: "?????»?µ ?????¶?µN? N??????µN??¶?°N?N? N????»N????? ?»?°N?????N??????µ ?±N?????N? ?? N???N?N?N?"
            },
            button: {
                quick: "??N?N?N?N??°N? ???°N?N?N????????°",
                netmap: "???°N?N??° N??µN???",
                detail_menu: "? ?°N?N???N??µ????N??µ ???°N?N?N?????????",
                USB: "USB-N?N?????N?????",
                about: "???± N?N????? N???N?N??µN??µ",
                back: "???°?·?°??",
                save: "????N?N??°????N?N?",
                next: "???°?»?µ?µ",
                login: "??????N???",
                main_menu: "???»?°???????µ ???µ??N?",
                manual: "???°N?N?N?????N?N? N?N?N?N?????N?N????? ??N?N?N???N?N?",
                start: "???°N??°N?N? N??°?±??N?N? ???°N?N??µN??°"
            },
            pending: {
                applying: "?????????¶????N??µ, ???°N?N?N????????? N???N?N??°??N?N?N?N?N?",
                done: "???°N?N?N????????? ??N??????µ???µ??N? N?N????µN?????"
            },
            title: {
                login: "????N???N????·?°N???N?",
                failed_login: "????N???N????·?°N???N? ???µ ????N?N?N??????°"
            },
            wan: {
                static: "??N??°N???N??µN??????? IP",
                dhcp: "DHCP",
                pppoe: "PPPOE",
                l2tp: "L2TP",
                bridge: "BRIDGE"
            },
            cpe_status: {
                connect: "?˜??N??µN????µN? ?????????»N?N??µ??",
                not_connected: "???µN? N????µ???????µ????N?",
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                router_getting_ip: "? ??N?N??µN? ?????»N?N??°?µN? IP-?°??N??µN?",
                connecting: "??N?N??°???°???»?????°?µN?N?N? ????N??µN????µN?-N????µ???????µ?????µ",
                no_wan: "???°?±?µ?»N? ???µ ?????????»N?N??µ??",
                ip_no_getted: "IP-?°??N??µN? ???µ ?????»N?N??µ??",
                no_auth: "???µ???µN??????µ ????N? ?????»N??·?????°N??µ?»N? ???»?? ???°N????»N?",
                no_resolve: "???µ N????°?µN?N?N? N??°?·N??µN???N?N? ????N? vpn-N??µN????µN??°"
            },
            W: {
                LANG_INVALID_IPV4_ADDR_SHOULD_NOT_EMPTY: "IP-?°??N??µN? ???µ ?????¶?µN? ?±N?N?N? ??N?N?N?N???! ?­N??? ?????»?¶??N? ?±N?N?N? ?·?°?????»???µ??N? N? 4 ?·???°N???N?N? N???N??µ?», ???°?? xxx.xxx.xxx.xxx.",
                LANG_INVALID_IPV4_ADDR_SHOULD_BE_DECIMAL_NUM: "???µ???µN???N??? IP-?°??N??µN? ?·???°N??µ?????µ. ?­N??? ?????»?¶???? ?±N?N?N? ???µN?N?N???N??????µ N???N??»?? (0-9).",
                LANG_INVALID_IPV4_ADDR: "???µ???µN???N??? IP-?°??N??µN? ?·???°N??µ?????µ.",
                LANG_INVALID_IPV4_ADDR_1ST_DIGIT: "???µ??????N?N?N?????N??? ?????°???°?·???? IP-?°??N??µN? ?? 1-?? N???N?N?N?. ?­N??? ?????»?¶???? ?±N?N?N? 0-255.",
                LANG_INVALID_IPV4_ADDR_2ND_DIGIT: "???µ??????N?N?N?????N??? ?????°???°?·???? IP-?°??N??µN? ?? 2 N???N?N?N?. ?­N??? ?????»?¶???? ?±N?N?N? 0-255.",
                LANG_INVALID_IPV4_ADDR_3RD_DIGIT: "???µ??????N?N?N?????N??? ?????°???°?·???? IP-?°??N??µN? ?? 3 N???N?N?N?. ?­N??? ?????»?¶???? ?±N?N?N? 0-255.",
                LANG_INVALID_IPV4_ADDR_4TH_DIGIT: "???µ??????N?N?N?????N??? ?????°???°?·???? IP-?°??N??µN? ?? 4 N???N?N?N?. ?­N??? ?????»?¶???? ?±N?N?N? 1-254.",
                LANG_INVALID_IPV4_SUBNET_SHOULD_NOT_EMPTY: "???°N????° ??????N??µN??? ???µ ?????¶?µN? ?±N?N?N? ??N?N?N?N???! ?­N??? ?????»?¶??N? ?±N?N?N? ?·?°?????»???µ??N? N? 4 ?·???°N???N?N? N???N??µ?», ???°?? xxx.xxx.xxx.xxx.",
                LANG_INVALID_IPV4_SUBNET_SHOULD_BE_DECIMAL_NUM: "???µ???µN??????µ ?·???°N??µ?????µ ???°N????? ??????N??µN???. ?­N??? ?????»?¶???? ?±N?N?N? ???µN?N?N???N??????µ N???N??»?? (0-9).",
                LANG_INVALID_IPV4_SUBNET_DIGIT: "???µ???µN???N??? ?·???°N???N??? ???°N????° ??????N??µN???. ?­N??? ?????»?¶???? ?±N?N?N? N???N??»?? 0, 128, 192, 224, 240, 248, 252 ???»?? 254.",
                LANG_INVALID_MAC_ADDR_SHOULD_NOT_EMPTY: "MAC-?°??N??µN? ???µ ?????¶?µN? ?±N?N?N? ??N?N?N?N???.",
                LANG_INVALID_MAC_ADDR_NOT_COMPLETE: "???????? MAC ?°??N??µN? ???µ N????»N??µN?N?N? ?????»??N???. ?????° ?????»?¶???° ?±N?N?N? 12 N???N?N? ?? N??µN?N????°??N??°N??µN???N??????? N???N????°N??µ.",
                LANG_INVALID_MAC_ADDR: "???????? MAC ?°??N??µN?.",
                LANG_INVALID_MAC_ADDR_SHOULD_BE: "???µ???µN???N??? ??????-?°??N??µN?. ???? ?????»?¶?µ?? ?±N?N?N? ?? N??µN?N????°??N??°N??µN???N??????µ N???N??»?? (0-9 ???»?? AF).",
                LANG_CONFIRM_DELETE_ONE_ENTRY: "??N? ???µ??N?N?????N??µ?»N????? N???N???N??µ N????°?»??N?N? ??N??±N??°????N?N? ?·?°????N?N??",
                LANG_CONFIRM_DELETE_ALL_ENTRY: "??N? ???µ??N?N?????N??µ?»N????? N???N???N??µ N????°?»??N?N? ??N??µ ?·?°????N??? ???",
                LANG_CONFIRM_DELETE: "??N? N????µN??µ??N?, N?N??? N???N???N??µ N????°?»??N?N??",
                LANG_INVALID_IPV6_PREFIX: "???µ???µN???N??? ??N??µN?????N? IPv6.",
                LANG_CHANGE_SETTING_SUCCESSFULLY: "?˜?·???µ????N?N? N?N????µN??????? N?N?N??°??????????!"
            },
            wizard: {
                model: "???????µ?»N? N?N?N?N?????N?N????°: ",
                ver: "???µN?N???N? N?N?N?N?????N?N????°: ",
                sw_ver: "???µN?N???N? ??N?????N??°???????????? ???±?µN????µN??µ????N?: ",
                mac: "M???? ?°??N??µN?: ",
                pppoe_name: "?˜??N? ?????»N??·?????°N??µ?»N? PPPoE: ",
                pppoe_pass: "???°N????»N? PPPoE: ",
                ser_info: "???µN?????N????°N? ????N???N????°N???N?: ",
                sn: "???µN???????N??? ???????µN?: ",
                login: "??????????",
                save: "????N?N??°????N?N?",
                pass: "???°N????»N? ?±?µN???N??????????????? N??µN???: ",
                enable5: "?????»N?N???N?N? ?±?µN???N???????????N?N? N??µN?N? 5 ????N? ",
                enable2: "?????»N?N???N?N? ?±?µN???N???????????N?N? N??µN?N? 2.4 ????N? ",
                wifi2: "?????°???°?·???? 2,4 ????N?",
                wifi5: "?????°???°?·???? 5 ????N?",
                ip: "IP ????N??µN? N?N?N?N?????N?N????°: ",
                login_rule: "?????????? ???»N? N???N??°???»?µ????N?: ",
                pass_rule: "???°N????»N? ???»N? N???N??°???»?µ????N?: ",
                name: "?˜??N? ?±?µN???N??????????????? N??µN???: ",
                descr: "??N? ?????¶?µN??µ ???·???µ????N?N? ????N? ?? ???°N????»N? N??????µ?? Wi-Fi N??µN??? ???»?? ??N????»N?N???N?N? Wi-Fi ?? N?N????? ???????µ.",
                apply_descr: "??N??????µ???µ?????µ ???°N?N?N????µ??. ??N?N?N?????N?N????? ?±N????µN? ????N?N?N??????? N??µN??µ?·",
                apply_descr_sec: " N??µ??N?????",
                apply_warning: "?????????°?????µ! ???µ ??N????»N?N??°??N??µ ????N??°?????µ N?N?N?N?????N?N????°!",
                done: "???°N?N?N????????° N?N????µN????? ?·?°???µN?N??µ???°, ??N??????µN???N?N? N??°?±??N?N? ?˜??N??µN????µN??° ?????¶???? ???°N??°?? N? ???°N??µ???? N??°??N??°",
                link: "???µN??µ??N??? ???° N??°??N? ? ??N?N??µ?»?µ??????",
                descr_fail: "???µ???µN????? N????°?·?°??N? ????N? ?????»N??·?????°N??µ?»N? ???»?? ???°N????»N? PPPoE, ??N??????µN?N?N??µ ?????µ???µ????N??µ ???°????N??µ ?? ???°?¶????N??µ ??N????????»?¶??N?N?",
                login_text: "?????µ????N??µ ????N? ?????»N??·?????°N??µ?»N?",
                pass_text: "?????µ????N??µ ???°N????»N?",
                pppoe_description: "?????¶?°?»N???N?N??°, ?????µ????N??µ ????N? ?????»N??·?????°N??µ?»N? ?? ???°N????»N?, ?????»N?N??µ????N??µ ??N? ??N??????°?????µN??°.",
                vlan_description: '???°?????»????N??µ ?????»?µ "???????µN? VLAN", ?µN??»?? ?????? ????N?N?N???????.',
                VLAN: "?????µ????N??µ ???????µN? VLAN",
                priority_VLAN: "?????µ????N??µ ??N?????N???N??µN? VLAN",
                lan1: "LAN 1",
                lan2: "LAN 2",
                lan3: "LAN 3",
                lan4: "LAN 4",
                tv_desc: '??N??±?µN???N??µ ????N?N?N?, ????N???N?N??µ ?±N???N?N? ??N??????»N??·?????°??N? ???»N? ?????????»N?N??µ????N? N??µ?»?µ?????·???????????? ??N???N?N??°?????? ?? ?·?°?????»????N??µ ?????»?µ "???????µN? VLAN", ?µN??»?? ?????? ????N?N?N???????. ????N??»?µ ???°N?N?N????????? ?????????»N?N???N??µ N??µ?»?µ?????·????????N?N? ??N???N?N??°????N? ?? ??N??±N??°????N??µ LAN ????N?N?N? N?N?N?N?????N?N????°.',
                tv_headline: "??N??±?µN???N??µ ????N?N? ?????????»N?N??µ????N? N??µ?»?µ?????·???????????? ??N???N?N??°??????",
                message: "??N?N??°?????????° ?·?°???µN?N??µ???°.",
                info: "???°????N???N??µ ???»?? ?·?°??????????N??µ ????N???N????°N???N? ?? ?????????»N?N??µ??????.",
                message_warning: "?????????°?????µ, ??N? ???·???µ?????»?? ???°N??°???µN?N?N? WiFi. ????N??»?µ ???°?¶?°N???N? ???????????? N???N?N??°????N?N? ???°?? ???µ???±N??????????? ?????????»N?N???N?N?N?N? ?? ?????????? N??µN??? WiFi.",
                voip_desc: '??N??±?µN???N??µ ????N?N?N?, ????N???N?N??µ ?±N???N?N? ??N??????»N??·?????°??N? ???»N? ?????????»N?N??µ????N? SIP N??µ?»?µN??????° ?? ?·?°?????»????N??µ ?????»?µ "???????µN? VLAN", ?µN??»?? ?????? ????N?N?N???????. ????N??»?µ ???°N?N?N????????? ?????????»N?N???N??µ SIP N??µ?»?µN????? ?? ??N??±N??°????N??µ LAN ????N?N?N? N?N?N?N?????N?N????°.',
                pppoe_warning: "???µ??N??°?????»N???N??? ?»???????? ???»?? ???°N????»N? ???»N? N????µ???????µ????N? PPPoE",
                nowan_warning: "??N?N?N?N?N?N???N??µN? WAN ?????????»N?N??µ?????µ.",
                nowan_desc: '??N??????µN?N?N??µ, ?????¶?°?»N???N?N??°, ??N??°?????»N?????N?N?N? ?????????»N?N??µ????N? WAN ???°?±?µ?»N?, ???°?? ???????°?·?°???? ?????¶?µ. ??N??»?? ??N????±?»?µ??N? ???µ N????°?»??N?N? N??µN???N?N?, ???±N??°N???N??µN?N? ?? N??»N??¶?±N? N??µN?????N??µN??????? ?????????µN??¶???? ?????? "? ??N?N??µ?»?µ??????"',
                pppoe_fail_warning: "???µ?????·?????¶???? N?N?N??°????????N?N? ?????????»N?N??µ?????µ ?? N??µN??? ?????? A«? ??N?N??µ?»?µ??????A»",
                pppoe_fail_desc: "??N??????µN?N?N??µ, ?????¶?°?»N???N?N??°, ??N??°?????»N?????N?N?N? ?????????»N?N??µ????N? WAN ???°?±?µ?»N? ?? ?????µ???µ????N?N? ???°????N?N?. ?? N??»N?N??°?µ ?µN??»?? ??N????±?»?µ??N? N??µN???N?N? ???µ N????°?»??N?N?, ???±N??°N???N??µN?N? ?? N??»N??¶?±N? N??µN?????N??µN??????? ?????????µN??¶???? ?????? A«? ??N?N??µ?»?µ??????A».",
                nowan_text_rostel: "??N? ?????¶?µN??µ ???°N?N?N?????N?N? ???°N?N?N?N?N????·?°N???N? N??°????N?N???N?N??µ?»N????? ???»?? ??N??? ????????N??? ??N?N?N????µ?????????? ???°N?N??µN??° ?±N?N?N?N????? ???°N?N?N?????????. ?????????»N?N???N??µ ???°?±?µ?»N?, ??N??????µ???µ????N??? ?? ???°??, ?? ????N?N? WAN ???°N??µ???? N?N?N?N?????N?N????°."
            },
            profiles: {
                title: "??N??????µN???N?N? N??µ????????",
                descr: "?????¶?°?»N???N?N??°, ??N??±?µN???N??µ ???°??N???N??µ?????????°?»N???N??? N????»???°?» ?? N????»???°?» ?????µ ??N? ???°N???????N??µN?N?.",
                region: "??N??±?µN???N??µ N??µ????????",
                subregion: "??N??±?µN???N??µ N????»???°?»",
                profile: "??N??±?µN???N??µ N?N??»N???N?"
            },
            acl: {
                title: "??????N?N????»N? ????N?N?N????°",
                description: "???° N?N????? N?N?N??°????N??µ ?????¶???? ???°N?N?N?????N?N? ??????N?N????»N? ????N?N?N????°.",
                port: "????N?N?:",
                protocol: "??N???N????????»:",
                ip: "IP ????N??µN? N?N?N?N?????N?N????°:",
                mask: "???°N????° ??????N??µN???:",
                interface: "?˜??N??µN?N??µ??N?:",
                web: "Web",
                telnet: "Telnet",
                ping: "????????"
            },
            buttons: {
                save: "????N?N??°????N?N? ?? ??N??????µ????N?N?",
                rm_selected: "?????°?»??N?N? ??N??±N??°???????µ"
            },
            notify: {
                send: "??N???N??°?????° ???°????N?N?",
                done: "????N???????"
            }
        };
    }, {} ],
    14: [ function(t, e, n) {
        "use strict";
        var i = {
            static: 0,
            dhcp: 1,
            pppoe: 3,
            l2tp: 6,
            bridge: 20
        };
        Object.freeze(i);
        var s = {
            internet: 2
        };
        Object.freeze(s), e.exports.AddressTypesEnum = i, e.exports.ServiceTypes = s, e.exports.adresstype_to_str = function(t) {
            switch (t) {
              case i.static:
                return "Static";

              case i.dhcp:
                return "DHCP";

              case i.pppoe:
                return "PPPoE";

              case i.l2tp:
                return "L2TP";

              case i.bridge:
                return "BRIDGE";

              default:
                console.error("Can't show this wan type");
            }
        }, e.exports.serviceTypesToStr = function(t) {
            return t !== s.internet ? "" : "INTERNET";
        };
    }, {} ],
    15: [ function(t, e, n) {
        "use strict";
        var i, s, o, r = t("./system.js").login_rpc, a = t("./dom-maker.js").WAN_STATUS_T, l = t("./dom-maker.js").AddressTypesEnum, c = {
            ru: t("./lang/ru.json"),
            en: t("./lang/en.json")
        };
        n.lang = function() {
            return i || c.ru;
        }, n.langTag = function() {
            return s;
        }, n.langOpts = function() {
            return o;
        }, n.init_lang_system = function() {
            return 1 == {
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            }.CONFIG_LUNA ? r("tr181_get", {
                path: "Device.UserInterface"
            }).then(function(t) {
                o = t.AvailableLanguages.split(",").map(function(t) {
                    return {
                        name: t,
                        value: t
                    };
                }), i = c[t.CurrentLanguage], s = t.CurrentLanguage;
            }).catch(function(t) {
                return console.log(t);
            }) : (i = c.ru, s = "ru", Promise.resolve(!0));
        }, n.wan_service_to_text = function(t) {
            var e = n.lang();
            switch (t) {
              case a.S_DISABLED:
              case a.S_DISCONNECTED:
                return e.cpe_status.ip_no_getted;

              case a.S_CONNECTING:
              case a.S_IN_IDLE:
              case a.S_REQ_IP:
                return e.cpe_status.connecting;

              case a.S_CONNECTED:
                return e.cpe_status.connect;

              case a.S_NO_AUTH:
                return e.cpe_status.no_auth;

              case a.S_NO_SERVER:
                return e.cpe_status.no_resolve;

              case a.S_NO_PADO:
              case a.S_NO_PADS:
              case a.S_NO_AC:
              case a.S_NO_IP:
              case a.S_ERROR:
                return e.cpe_status.ip_no_getted;

              default:
                return "";
            }
            return "";
        }, n.wan_status_to_text = function(t) {
            var e = n.lang();
            switch (t) {
              case a.S_DISABLED:
              case a.S_DISCONNECTED:
                return e.cpe_status.ip_no_getted;

              case a.S_CONNECTING:
              case a.S_IN_IDLE:
              case a.S_REQ_IP:
                return e.cpe_status.connecting;

              case a.S_CONNECTED:
                return e.cpe_status.connect;

              case a.S_NO_AUTH:
                return e.cpe_status.no_auth;

              case a.S_NO_SERVER:
                return e.cpe_status.no_resolve;

              case a.S_NO_PADO:
              case a.S_NO_PADS:
              case a.S_NO_AC:
              case a.S_NO_IP:
              case a.S_ERROR:
                return e.cpe_status.ip_no_getted;

              default:
                return "";
            }
            return "";
        }, n.wan_type_to_text = function(t) {
            var e = n.lang();
            switch (t) {
              case l.static:
                return e.wan.static;

              case l.dhcp:
                return e.wan.dhcp;

              case l.pppoe:
                return e.wan.pppoe;

              case l.l2tp:
                return e.wan.l2tp;

              case l.bridge:
                return e.wan.bridge;

              default:
                return "";
            }
        };
    }, {
        "./dom-maker.js": 5,
        "./lang/en.json": 12,
        "./lang/ru.json": 13,
        "./system.js": 23
    } ],
    16: [ function(t, e, n) {
        "use strict";
        function i(t) {
            t || (this.root = "bee-quick-menu.html?"), this.external_urls = [], this.urls = [], 
            this._listen();
        }
        i.prototype._listen = function() {
            var e = this;
            window.addEventListener("popstate", function(t) {
                e._go(t.state);
            });
        }, i.prototype.go = function(t) {
            this._go_external(t) || (window.history.pushState(t, "Title", this.root + t), this._go(t));
        }, i.prototype.href = function(t, e) {
            return this.external_urls.push({
                r: t,
                href: e
            }), this;
        }, i.prototype.on = function(t, e) {
            return this.urls.push({
                r: t,
                handler: e
            }), this;
        }, i.prototype.set_root = function(t) {
            return this.root = t, this;
        }, i.prototype.default = function(t) {
            return this.default = t, this;
        }, i.prototype.reload = function(t) {
            this._reload ? this._reload(t) : window.location.href = window.location.href;
        }, i.prototype._go_external = function(e) {
            var t = !!e && this.external_urls.find(function(t) {
                return e.match(t.r);
            });
            return !!t && (window.location.href = t.href, !0);
        }, i.prototype._go = function(e) {
            var t = !!e && this.urls.find(function(t) {
                return e.match(t.r);
            });
            if (!t) return this.default(), this;
            t.handler();
        }, i.prototype.start = function(t) {
            return this._go(window.location.href), this;
        }, e.exports.NaVi = i;
        e.exports.navi = function() {
            return e.exports.navi._navi || (e.exports.navi._navi = new i()), e.exports.navi._navi;
        }, e.exports.NaVi = i;
    }, {} ],
    17: [ function(t, e, n) {
        "use strict";
        t("./dom-maker.js").CloneMacWidget;
        var r = t("./dom-maker.js").concat_arr, a = t("./dom-maker.js").generate_clone_mac_simple, i = t("./data_utility.js"), l = i.wlanClientList, c = i.lanClientList, u = t("../../packages/nanolib/js/nano-json-rpc-2.js"), d = (t("../../packages/nanolib/js/nano-dom.js"), 
        t("./form-widgets.js").SinglApmibRadioForm), p = t("./form-widgets.js").single_apmib_checkbox_form, h = t("./form-widgets.js").AddRuleFormInputs, _ = (t("./form-widgets.js").RmListForm, 
        t("./virtual-dom.js").VirtualDom);
        function s() {
            this.tree = new _("div"), this.tree.root().set_class("blocks-content").up();
        }
        function o() {
            this.tree = new _("div"), this.tree.root().set_class("blocks-row").up();
        }
        function f() {
            this.tree = new _("div"), this.tree.root().set_class("blocks-row blocks-row_autoWidth").up();
        }
        function m() {
            this.tree = new _("div"), this.tree.root().set_class("blocks-col blocks-leftPart").up();
        }
        function v(t, e) {
            e = (e.attr || {}).add_className || "";
            this.tree = new _("div"), this.tree.root().set_class("blocks-col blocks-rightPart " + e).up();
        }
        function b(t, e) {
            this.exports = {
                a: "a",
                cb: {}
            };
            e.attr;
            this.tree = new _("label"), this.tree.root().child("span").set_class("switch").child("checkbox").bind(this.exports.cb).up().child("span").child("span").child("span");
        }
        function g(t, e) {
            this.exports = {
                input: {}
            };
            e.attr;
            this.tree = new _("label"), this.tree.root().child("radio").bind(this.exports.input).up().child("span");
        }
        function x(t, e) {
            var n = e.attr || {}, e = n.input || "input_text";
            this.tree = new _(e, n.attr), n.bind && this.tree.root().bind(n.bind);
        }
        function w(t, e) {
            e.attr;
            var n = e.attr.left || {
                component: "label",
                attr: {
                    text: "l"
                }
            }, i = e.attr.right || {
                component: "label",
                attr: {
                    text: "r"
                }
            }, e = e.attr.rootComponentName || "blocks-row";
            this.tree = new _("div"), this.tree.root().child(e).child("blocks-leftPart").child(n.component, n.attr).up().up().child("blocks-rightPart", {
                add_className: i.add_className
            }).child(i.component, i.attr).up().up().up();
        }
        function y(t, e) {
            return {
                left: {
                    component: "label",
                    attr: {
                        text: t
                    }
                },
                right: {
                    component: "bind-input",
                    attr: {
                        input: "input_text",
                        bind: e
                    }
                }
            };
        }
        function j(t, e) {
            return {
                rootComponentName: "blocks-row_autoWidth",
                left: {
                    component: "label",
                    attr: {
                        text: t
                    }
                },
                right: {
                    component: "bind-input",
                    add_className: "blocks-col_radioLine",
                    attr: {
                        input: "nbn-radio",
                        bind: e
                    }
                }
            };
        }
        function k(t, e) {
            var n = {}, i = this.exports = {}, s = e.attr || {}, e = s.text || "", o = s.mib || "";
            this.obj = {
                created: function() {
                    i.checkbox = n.exports.cb.el, i.form = p(i.checkbox, o);
                }
            }, this.tree = new _("nbn-blocks-row", {
                rootComponentName: "blocks-row_autoWidth",
                left: {
                    component: "label",
                    attr: {
                        text: e
                    }
                },
                right: {
                    component: "bind-input",
                    attr: {
                        input: "nbn-checkbox",
                        bind: n
                    }
                }
            });
        }
        function N(t, e) {
            var n = [], i = this.exports = {}, e = e.attr || {}, s = (e.method_add, e.radio_name || "radiotest"), o = e.mib || "radiotest", e = e.radio_list || [];
            this.obj = {
                created: function() {
                    n.forEach(function(t) {
                        return t.input.exports.input.el.name(s);
                    });
                },
                mounted: function() {
                    console.log(n);
                    var t = n.map(function(t) {
                        return {
                            value: t.value,
                            input: t.input.exports.input.el
                        };
                    });
                    i.form = new d(t, o);
                }
            }, this.tree = new _("div");
            var r = this.tree.root();
            e.forEach(function(t) {
                var e = {
                    value: t.value,
                    input: {}
                };
                n.push(e), r.child("nbn-blocks-row", j(t.text, e.input)).up();
            });
        }
        function I(t, e) {
            var n = {}, e = (this.exports = {}, e.attr || {}), i = e.setter || "", s = e.text || "";
            e.mib;
            function o() {
                return Promise.all([ l(), c() ]).then(function(t) {
                    return t.reduce(r);
                });
            }
            this.obj = {
                mounted: function() {
                    a(n.el, s, o, i);
                }
            }, this.tree = new _("div"), this.tree.root().bind(n);
        }
        function E(t, e) {
            var n = {}, i = {}, s = this.exports = {}, o = e.attr || {}, r = o.method_add || "", a = {};
            this.obj = {
                created: function() {
                    a = s.form = new h([ n.el, i.el ], function() {
                        return u(r, {
                            mac: n.el.e.value,
                            comment: i.el.e.value
                        });
                    });
                }
            }, this.tree = new _("div"), this.tree.root().child("nbn-blocks-row", y("MAC-?°??N??µN?", n)).up().child("nbn-blocks-row", y("?????????µ??N??°N?????", i)).up().child("clone-mac-lan-wlan-simple", {
                text: "?????±?°????N?N? N?N?N?N?????N?N?????",
                setter: function(t, e) {
                    n.el.value(t), i.el.value(e), o.submit && o.submit.el.disabled(!1), a.change();
                }
            }).up();
        }
        e.exports.nbn_row_input_attr = y, e.exports.nbn_row_radio_attr = j, e.exports.registry = function(t) {
            return t.registry("blocks-content", s).registry("blocks-row", o).registry("blocks-row_autoWidth", f).registry("blocks-rightPart", v).registry("blocks-leftPart", m).registry("nbn-checkbox", b).registry("nbn-radio", g).registry("bind-input", x).registry("nbn-blocks-row", w).registry("single-apmib-checkbox", k).registry("single-apmib-radio", N).registry("clone-mac-lan-wlan-simple", I).registry("add-mac-to-table-form", E);
        };
    }, {
        "../../packages/nanolib/js/nano-dom.js": 138,
        "../../packages/nanolib/js/nano-json-rpc-2.js": 139,
        "./data_utility.js": 4,
        "./dom-maker.js": 5,
        "./form-widgets.js": 9,
        "./virtual-dom.js": 26
    } ],
    18: [ function(t, e, n) {
        "use strict";
        var i = t("../../packages/nanolib/js/os.js");
        function r() {
            return window.XMLHttpRequest ? new ("onload" in new XMLHttpRequest() ? XMLHttpRequest : XDomainRequest)() : new ActiveXObject("Microsoft.XMLHTTP");
        }
        e.exports.call_and_poll = function(t, e) {
            return e(), i.poll(t, e);
        }, document.addEventListener("DOMContentLoaded", function() {
            !function() {
                for (var t = document.querySelectorAll("input[type='password']"), e = 0; e < t.length; e++) {
                    var n = document.createElement("span");
                    n.classList.add("showPassword"), n.addEventListener("click", function() {
                        this.previousElementSibling.type = "password" == this.previousElementSibling.type ? "text" : "password";
                    }), t[e].parentElement.appendChild(n);
                }
            }();
            var t = document.querySelectorAll(".latin");
            t && (t = Array.prototype.slice.call(t, 0)).forEach(function(t) {
                t.addEventListener("keydown", function(t) {
                    -1 != t.key.search(/[?°-N???-??N???]/g) && t.preventDefault();
                });
            });
        });
        e.exports.await_forEach = function(t, n) {
            return t.reduce(function(t, e) {
                return t.then(function() {
                    return n(e);
                });
            }, Promise.resolve(!0));
        }, e.exports.step_by_step = function() {
            return {
                list: [],
                push_back: function(t) {
                    this.list.push(t);
                },
                push_front: function(t) {
                    this.list.unshift(t);
                },
                clear: function() {
                    this.list = [];
                },
                call_all: function() {
                    return this.list.reduce(function(t, e) {
                        return t.then(e);
                    }, Promise.resolve(!0));
                }
            };
        }, e.exports.spread_form = function(t) {
            if (!t) return [];
            if (!t.elements) return [];
            for (var e = [], n = 0; n < t.elements.length; n++) e.push(t.elements[n]);
            return e;
        }, e.exports.disableTextField = function(t) {
            document.all || document.getElementById ? t.disabled = !0 : (t.oldOnFocus = t.onfocus, 
            t.onfocus = skip);
        }, e.exports.enableTextField = function(t) {
            document.all || document.getElementById ? t.disabled = !1 : t.onfocus = t.oldOnFocus;
        }, e.exports.getJSON = function(i) {
            return new Promise(function(t, e) {
                var n = r();
                n.open("GET", i, !0), n.addEventListener("load", function() {
                    n.status < 400 ? t(JSON.parse(n.responseText)) : e(new Error("Request failed: " + n.statusText));
                }), n.addEventListener("error", function() {
                    e(new Error("Network error"));
                }), n.send(null);
            });
        }, e.exports.getHTML = function(t, s) {
            return s = void 0 === s ? "string" : "dom", new Promise(function(e, n) {
                var i = r();
                i.open("GET", t, !0), i.addEventListener("load", function() {
                    var t;
                    i.status < 400 ? ("dom" == s && (t = new DOMParser().parseFromString(i.responseText, "text/html"), 
                    e(t.body.firstElementChild)), e(i.responseText)) : n(new Error("Request failed: " + i.statusText));
                }), i.addEventListener("error", function() {
                    n(new Error("Network error"));
                }), i.send(null);
            });
        }, e.exports.postForm = function(i) {
            for (var s, o = "", t = 0; t < i.elements.length; t++) {
                var e = i.elements[t];
                "submit" == e.type && (s = e), "checkbox" != e.type && "radio" != e.type || !e.checked ? "checkbox" != e.type && "radio" != e.type && (o += encodeURIComponent(i.elements[t].name) + "=" + encodeURIComponent(i.elements[t].value), 
                t != i.elements.length - 1 && (o += "&")) : (o += encodeURIComponent(i.elements[t].name) + "=" + encodeURIComponent(i.elements[t].value), 
                t != i.elements.length && (o += "&"));
            }
            return s.classList.add("pending"), "multipart/form-data" == i.enctype && (o = new FormData(i)), 
            console.dir(o), console.dir(i.action), new Promise(function(t, e) {
                var n = r();
                n.open("POST", i.action, !0), "application/x-www-form-urlencoded" == i.enctype && n.setRequestHeader("Content-type", i.enctype), 
                n.addEventListener("load", function() {
                    n.status < 400 ? (t(n.responseText), console.dir(n.responseText)) : (e(new Error("Request failed: " + n.statusText)), 
                    console.dir(n.statusText)), s.classList.remove("pending"), s.disabled = !0;
                }), n.addEventListener("error", function() {
                    e(new Error("Network error")), s.classList.remove("pending");
                }), n.send(o);
            });
        }, e.exports.formDefaultSubmitDisable = function(t) {
            function e(t) {
                for (var e = t.form, n = !0, i = 0; i < e.elements.length; i++) !function(t) {
                    var e = !0;
                    switch (t.tagName) {
                      case "SELECT":
                        var n, i = !1;
                        if (t.length) {
                            for (var s = 0; s < t.length; s++) t[s].selected && (n = s), t[s].defaultSelected && (i = !0), 
                            t[s].selected != t[s].defaultSelected && (e = !1);
                            i || (t[0].defaultSelected = !0, t[n].selected = !0);
                        }
                        return e;

                      case "INPUT":
                        switch (t.type) {
                          case "checkbox":
                          case "radio":
                            t.checked != t.defaultChecked && (e = !1);
                            break;

                          case "file":
                          case "text":
                          case "password":
                            t.value != t.defaultValue && (e = !1);
                            break;

                          case "submit":
                            return e;

                          default:
                            console.warn("Develop warn! input.type " + t.type + " unexpected");
                        }
                        break;

                      case "TEXTAREA":
                        t.value != t.defaultValue && (e = !1);
                        break;

                      case "BUTTON":
                        return e;

                      default:
                        console.warn("Develop warn! input.tagName " + t.tagName + " unexpected");
                    }
                    return e;
                }(e.elements[i]) && (n = !1);
                return n;
            }
            for (var n, i, s = 0; s < t.length; s++) "submit" == (i = t.elements[s]).type ? (n = i).disabled = !0 : i.addEventListener("input", function() {
                var t = e(this);
                console.log(t), n.disabled = !!t;
            });
        }, e.exports.isDefaultForm = function(t) {
            for (var e, n = t.form, i = !0, s = 0; s < n.elements.length; s++) {
                var o = n.elements[s];
                "submit" == o.type && (e = o), function(t) {
                    if ("select" != t.localName) return t.value == t.defaultValue && t.checked == t.defaultChecked;
                    var e, n = !0, i = !1;
                    if (t.length) {
                        for (var s = 0; s < t.length; s++) t[s].selected && (e = s), t[s].defaultSelected && (i = !0), 
                        t[s].selected != t[s].defaultSelected && (n = !1);
                        i || (t[0].defaultSelected = !0, t[e].selected = !0);
                    }
                    return n;
                }(o) || (i = !1);
            }
            e.disabled = i;
        }, e.exports.getHttpRequest = r;
    }, {
        "../../packages/nanolib/js/os.js": 146
    } ],
    19: [ function(t, e, n) {
        "use strict";
        function i() {}
        i.prototype.setGlobalNotify = function(t) {
            this.gNotify = t;
        }, i.prototype.applying = function(t) {
            this.gNotify && this.gNotify.exports.run(t);
        }, i.prototype.done = function(t) {
            this.gNotify && this.gNotify.exports.done();
        }, i.prototype.error = function(t) {
            this.gNotify && this.gNotify.exports.error(t);
        }, e.exports.notify_sys = function() {
            return e.exports.notify_sys._notSys || (e.exports.notify_sys._notSys = new i()), 
            e.exports.notify_sys._notSys;
        };
    }, {} ],
    20: [ function(t, e, n) {
        "use strict";
        var o = t("./nbn_lib.js"), r = t("libutillity");
        e.exports.simple_cached_call_and_poll = function(t, e, n) {
            var i, s = (n = n, i = new r.AutoCache(n), function(t) {
                return i.caching(t);
            });
            return o.call_and_poll(t, function() {
                return e().then(s);
            });
        };
    }, {
        "./nbn_lib.js": 18,
        libutillity: 180
    } ],
    21: [ function(t, e, n) {
        "use strict";
        Array.prototype.find || Object.defineProperty(Array.prototype, "find", {
            value: function(t) {
                if (null == this) throw new TypeError('"this" is null or not defined');
                var e = Object(this), n = e.length >>> 0;
                if ("function" != typeof t) throw new TypeError("predicate must be a function");
                for (var i = arguments[1], s = 0; s < n; ) {
                    var o = e[s];
                    if (t.call(i, o, s, e)) return o;
                    s++;
                }
            },
            configurable: !0,
            writable: !0
        }), Object.assign || Object.defineProperty(Object, "assign", {
            enumerable: !1,
            configurable: !0,
            writable: !0,
            value: function(t, e) {
                if (null == t) throw new TypeError("Cannot convert first argument to object");
                for (var n = Object(t), i = 1; i < arguments.length; i++) {
                    var s = arguments[i];
                    if (null != s) for (var o = Object.keys(Object(s)), r = 0, a = o.length; r < a; r++) {
                        var l = o[r], c = Object.getOwnPropertyDescriptor(s, l);
                        void 0 !== c && c.enumerable && (n[l] = s[l]);
                    }
                }
                return n;
            }
        });
    }, {} ],
    22: [ function(t, e, n) {
        "use strict";
        var i = t("system.js").no_login_rpc, s = {};
        e.exports.no_login_static_info = function(t) {
            return s.static_info ? s.static_info[t] || (console.error("static info id is not found", t), 
            "") : (console.error("static info is not loaded"), "");
        }, e.exports.update = function() {
            i("no_login_rpc_apmib_get", {
                list: [ "model_name", "fw_version", "hw_version", "mac_address", "serial_number" ]
            }).then(function(t) {
                s.static_info = t;
            }).catch(function(t) {
                console.error("can't load static data from server, detail:", t);
            });
        };
    }, {
        "system.js": 23
    } ],
    23: [ function(t, e, n) {
        "use strict";
        var i = t("../../packages/nanolib/js/nano-json-rpc-2.js"), s = t("../../packages/nanolib/js/nano-json-rpc-2.js").login_json_rpc, o = t("../../packages/nanolib/js/os.js"), r = t("../../packages/nanolib/js/nano-dom.js"), t = t("../../packages/nanolib/js/nano-ajax.js");
        function a(t) {
            this.notify = t;
        }
        a.prototype.run = function() {
            this.pending = this.notify.exports.status_pending("??N???N??°?????° ???°????N?N?");
        }, a.prototype.stop = function() {
            this.pending.stop();
        }, a.prototype.good = function() {
            var t = this;
            this.pending.stop(), this.notify.exports.good("????N???????"), setTimeout(function() {
                t.notify.exports.clear();
            }, 2e3);
        }, e.exports.rpc = i, e.exports.login_rpc = s, e.exports.no_login_rpc = s, e.exports.poll = o.poll, 
        e.exports.Pending = a, e.exports.makePending = function(t) {
            return new a(t);
        }, e.exports.$ = r, e.exports.ajax = t, e.exports.app = {
            rpc: i,
            login_json_rpc: s
        };
    }, {
        "../../packages/nanolib/js/nano-ajax.js": 137,
        "../../packages/nanolib/js/nano-dom.js": 138,
        "../../packages/nanolib/js/nano-json-rpc-2.js": 139,
        "../../packages/nanolib/js/os.js": 146
    } ],
    24: [ function(t, e, n) {
        "use strict";
        var i = t("./event-emitter.js").EventEmiter, s = t("./system.js").login_rpc, o = t("./system.js").poll;
        function r() {
            this._active = !1, this._blocked = !1, this._stoped = !1, this.twz_is_enable = !1, 
            this.code = 0, this.blocked = !1, this.ee = new i(), this.code_to_event = function(t) {
                return "twz-no-wan";
            };
        }
        function a(e) {
            return function() {
                if (!e._blocked) return s("twz", {}).then(function(t) {
                    e.twz_is_enable = t.twz_is_enable, t.twz_code != e.code && (e.code = t.twz_code, 
                    e._active = !1), e._active || !t.twz_is_enable || e._stoped ? e._active && !t.twz_is_enable && (e.code = -1, 
                    e._active = !1, e.ee.emit("twz-stop")) : (e._active = !0, e.ee.emit(e.code_to_event(e.code)));
                });
            };
        }
        r.prototype.on = function(t, e) {
            return this.ee.on(t, e), this;
        }, r.prototype.is_active = function() {
            return this._active;
        }, r.prototype.start = function() {
            a(this)(), this._pool = o(1e3, a(this));
        }, r.prototype.stop = function() {
            this._active = !1, this._stoped = !0;
            var e = this;
            s("twz_stop_temprary", {}).then(function(t) {
                return e.ee.emit("twz-stop");
            });
        }, r.prototype.set_code_converter = function(t) {
            return this.code_to_event = t, this;
        }, r.prototype.stop_poll = function(t) {
            this._pool && this._pool.cancel();
        }, r.prototype.block = function(t) {
            this._blocked = t, this._active = !1, this.ee.emit("twz-stop");
        }, e.exports.twz = function() {
            return e.exports.twz._twz || (e.exports.twz._twz = new r()), e.exports.twz._twz;
        };
    }, {
        "./event-emitter.js": 8,
        "./system.js": 23
    } ],
    25: [ function(e, t, n) {
        "use strict";
        var i, s = {
            PORTFW_ERROR_IP: -1,
            PORTFW_ERROR_SUBNET: -2,
            PORTFW_ERROR_FROMPORT: -3,
            PORTFW_ERROR_TOPORT: -4,
            PORTFW_ERROR_EXTERNEL_FROMPORT: -5,
            PORTFW_ERROR_EXTERNEL_TOPORT: -6,
            PORTFW_ERROR_RANGE: -7,
            PORTFW_EXTERNEL_RANGE: -8,
            PORTFW_ERROR_PROTOCOL: -9,
            PORTFW_ERROR_EXTERNEL_IP: -10,
            PORTFW_ERROR_COMMENT: -11,
            PORTFW_ERROR_MAX_RULES: -12,
            PORTFW_ERROR_RANGE_OVERLAP: -13
        };
        function o(t, e, n, i) {
            this.type = t, this.hash = t && t + e, this.comment = n, this.opts = i;
        }
        function r() {
            var t = Object.create(null);
            if (!arguments.length) return t;
            for (var e = 0, n = arguments, i = n.length; e < i; e += 2) t[n[e]] = n[e + 1];
            return t;
        }
        function a(t, e, n) {
            return new o("invalid", e || "", t || "", n);
        }
        function l() {
            return new o("valid");
        }
        Object.freeze(s), (i = o.prototype = r()).isValid = function() {
            return !this.type;
        }, i.isEqual = function(t) {
            return this.hash === (t && t.hash);
        };
        var c = r();
        function u(t, e, n, i) {
            function s(t) {
                return !e.test(t) && a(n || "");
            }
            s.sample = i, t.split(",").forEach(function(t) {
                c[t] = s;
            });
        }
        function d(t, e) {
            return t.sample = e, t;
        }
        u("int,port", /^-?\d+$/, " `FIELD_INT_INVALID`", " `FIELD_INT_SAMPLE`"), u("ports", /^\d+\s*(?:-\s*\d+)?(?:\s*,\s*\d+\s*(?:-\s*\d+)?)*$/, " `FIELD_PORTS_INVALID`", " `FIELD_PORTS_SAMPLE`"), 
        u("phone", /^[\d\*#\(),+wpdt\s-]+$/, " `FIELD_PHONE_INVALID`"), u("WPSPin", /^\d{8}$/, " `FIELD_WPSPIN_INVALID`"), 
        u("pin", /^\d{4,8}$/, " `FIELD_PIN_INVALID`"), u("ascii", /^[\x20-\x7E]+$/, " `FIELD_ASCII_INVALID`"), 
        u("ssid", /^[\x20-\x7E]{1,32}$/, " `FIELD_SSID_INVALID`"), u("login", /^[\w+()_=@.\/-]+$/, " `FIELD_LOGIN_INVALID`"), 
        u("user", /^[a-z0-9_]+$/i, " `FIELD_USER_INVALID`"), u("hostname", /^[ !#$%&\x27(),-\.0-9<=>@A-Z\[\]^_`a-z{}~]{1,15}$/, " `FIELD_HOSTNAME_INVALID`"), 
        u("password", /^[\x21-\x7E]+$/, " `FIELD_PASSWORD_INVALID`"), u("printable,wpaPsk", /^[\x20-\x7E]+$/, " `FIELD_PRINTABLE_INVALID`"), 
        u("MAC", /^[\da-f]{2}(?::[\da-f]{2}){5}$/i, " `FIELD_MAC_INVALID`", " `FIELD_MAC_SAMPLE`"), 
        u("mac", /^[\da-f]{2}([:\-]?[\da-f]{2}){5}$/i, " `FIELD_MAC_INVALID`", " `FIELD_MAC_SAMPLE`"), 
        u("ip,ipAddress,ipGateway", /^(?:(?:\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.){3}(?:\d{1,2}|1\d\d|2[0-4]\d|25[0-5])$/, " `FIELD_IP_INVALID`"), 
        u("ipPool", /^(?:(?:\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.){3}(?:\d{1,2}|1\d\d|2[0-4]\d|25[0-5])(?:-(?:1?\d{1,2}|2(?:[0-4]\d|5[0-5])))?$/, " `FIELD_IP_POOL_INVALID`"), 
        u("net_ip", /^(\d{1,3}\.){1,3}[\d]{1,8}(\/\d{1,2})?$/i, " `FIELD_NETIP_INVALID`", " `FIELD_NETIP_SAMPLE`"), 
        u("ip6", /^((?=.*::)(?!.*::.+::)(::)?([\dA-F]{1,4}:(:|\b)|){5}|([\dA-F]{1,4}:){6})((([\dA-F]{1,4}((?!\3)::|:\b|$))|(?!\2\3)){2}|(((2[0-4]|1\d|[1-9])?\d|25[0-5])\.?\b){4})$/i, " `FIELD_IPV6_INVALID`"), 
        u("ip6_pref", /^((?=.*::)(?!.*::.+::)(::)?([\dA-F]{1,4}:(:|\b)|){5}|([\dA-F]{1,4}:){6})((([\dA-F]{1,4}((?!\3)::|:\b|$))|(?!\2\3)){2}|(((2[0-4]|1\d|[1-9])?\d|25[0-5])\.?\b){4})(\/1[0-1][0-9]$)|(\/12[0-7]$)|(\/[1-9][0-9]?$)$/i, " `FIELD_IPV6PREF_INVALID`"), 
        u("domain", /^[a-z0-9\-]+(\.[a-z0-9\-]+)*$/i, " `FIELD_DOMAIN_INVALID`"), u("host,server", /^[a-z0-9\-]+(\.[a-z0-9\-]+)*|(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])(\.(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])){3}$/i, " `FIELD_HOST_INVALID`"), 
        u("socket", /^([a-z0-9\-]+(\.[a-z0-9\-]+)*|(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])(\.(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])){3})(:[\d]{1,5})?$/i, " `FIELD_SOCKET_INVALID`"), 
        u("URL", /^(https?:\/\/)?([\w\.\-]+(:[\w\.\-]+)?@)?([a-z][\w\-]+(\.[\w\-]+)*|(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])(\.(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])){3})(:[\d]{1,5})?(\/([\w\-_@\.+!,]|(\%[\da-f]{2}))+)*\/?(\?([\w$\-_@\.&+!*\(\),=]|\%[\da-z]{2})*)?(#([\w$\-_@\.&+!*\(\)\/,=]|\%[\da-z]{2})*)?$/i, " `FIELD_URL_INVALID`"), 
        u("URI", /^[a-z]+:\/{0,2}([\w\.\-]+(:[\w\.\-]+)?@)?([a-z][\w\-]+(\.[\w\-]+)*|(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])(\.(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])){3})(:[\d]{1,5})?(\/([\w\-_@\.+!,]|(\%[\da-f]{2}))+)*\/?(\?([\w$\-_@\.&+!*\(\),=]|\%[\da-z]{2})*)?(#([\w$\-_@\.&+!*\(\)\/,=]|\%[\da-z]{2})*)?$/i, " `FIELD_URI_INVALID`"), 
        u("local_URI", /^(?:(?:[a-z]+:\/{0,2})?([\w\.\-]+(:[\w\.\-]+)?@)?([a-z][\w\-]+(\.[\w\-]+)*|(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])(\.(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])){3})(:[\d]{1,5})?)?(\/([\w\-_@\.+!,]|(\%[\da-f]{2}))+)*\/?(\?([\w$\-_@\.&+!*\(\),=]|\%[\da-z]{2})*)?(#([\w$\-_@\.&+!*\(\)\/,=]|\%[\da-z]{2})*)?$/i, " `FIELD_LOCAL_URI_INVALID`"), 
        u("email", /^[-a-z0-9~!$%^&*_=+}{\'?]+(\.[-a-z0-9~!$%^&*_=+}{\'?]+)*@([a-z0-9_][-a-z0-9_]*(\.[-a-z0-9_]+)*\.([a-z]{2}[a-z0-9-]*)|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,5})?$/i, " `FIELD_EMAIL_INVALID`"), 
        u("folder", /^(\/[\s\w\-\.\_\+\(\)\`\'\"\[\]:\u0401\u0410-\u044F\u0451]+)*$/i, " `FIELD_FOLDER_INVALID`"), 
        u("date", /^20\d\d-(?:0[1-9]|1[012])-(?:0[1-9]|[12]\d|3[01])$/, " `FIELD_DATE_INVALID`", " `FIELD_DATE_SAMPLE`"), 
        u("time", /^(?:[01]\d|2[0-3]):(?:[0-5]\d):(?:[0-5]\d)$/, " `FIELD_TIME_INVALID`", " `FIELD_TIME_SAMPLE`"), 
        u("atcmd", /^AT[\x20-\x7F]*$/i, " `FIELD_ATCMD_INVALID`"), u("hex_rgb", /^#[0-9A-F]{6}$/i, " `FIELD_HEXRGB_INVALID`");
        var p = {
            int: function(e) {
                var n = e.min, i = e.max;
                return function(t) {
                    return ((t = 0 | t) < n || i < t) && a("FIELD_INT_INVALID_BOUNDS", "$", e);
                };
            },
            port: function(t) {
                return p.int({
                    min: t.min || 1,
                    max: t.max || 65535
                });
            },
            ips: function(t) {
                return d(function(t) {
                    var n;
                    if (t.split(/\s*,\s*/).some(function(t, e) {
                        if (c.ip(t)) return n = a("FIELD_IPS_INVALID", e, {
                            index: e
                        }), 1;
                    })) return n;
                }, " `FIELD_IPS_SAMPLE`");
            },
            uris: function(t) {
                return d(function(t) {
                    var n;
                    if (t.split(/\s*,\s*/).some(function(t, e) {
                        if (c.socket(t)) return n = a("FIELD_URIS_INVALID", e, {
                            index: e
                        }), 1;
                    })) return n;
                }, " `FIELD_URIS_SAMPLE`");
            },
            ipGateway: function(t) {
                var t = (t.deps || "/").split(","), i = t[0], s = t[1], o = e("ipv4");
                return function(t, e) {
                    var n = o.str2num(e[i]), e = o.width2num(+e[s]), t = o.str2num(t);
                    return (t & e) != (n & e) ? a("FIELD_GATEWAY_NOT_IN_THE_NET", "1") : t == n ? a("FIELD_GATEWAY_IS_EQ_NETIP", "2") : void 0;
                };
            },
            wpaPsk: function(t) {
                return function(t, e) {
                    t = t.length;
                    return 64 !== t || /^[\da-f]+$/.test() ? t < 8 ? a("FIELD_WPAPSK_TOOSHORT") : void 0 : a("FIELD_WPAPSK_HEX_INVALID");
                };
            },
            password: function(t) {
                var n = t.match;
                if (n) return function(t, e) {
                    return e[n] !== t && a("FIELD_PASSWORD_PAIR_NOT_MATCHED", "1");
                };
            },
            no_empty: function(t) {
                return function(t, e) {
                    return console.log(t), 0 == t.length && a("EMPTY", "1");
                };
            }
        };
        function h(t) {
            return "" === t && new o("required", "", "");
        }
        function _(t) {
            return "" === t && l();
        }
        function f(t) {
            return 0 === t && l();
        }
        function m(t, e) {
            var n = e.required ? h : _, i = c[t], s = e.zero ? f : void 0, o = p[t] && p[t](e), e = function(t, e) {
                return n(t) || s && s(t) || i && i(t) || o && o(t, e) || l();
            };
            return e.sample = o && o.sample || i && i.sample, e;
        }
        function v(t) {
            this.__data = t;
        }
        function b(t) {
            return console.log(t), "required" == t.type ? "???µ ?·?°???°??." : "invalid" == t.type ? "?·?°???°?? ???µ ????N?N??µ??N?????." : "";
        }
        n.Valid = l, n.getValidator = m, window.getValidator = m, v.prototype.valid = function(t, e) {
            e = m(t, e);
            return this.state && this.state.type && "valid" != this.state.type || (this.state = e(this.__data)), 
            this;
        }, v.prototype.data = function(t, e) {
            return this.state && this.state.type && "valid" != this.state.type || (this.__msg = e, 
            this.__data = t, this.__input = void 0), this;
        }, v.prototype.input_component = function(t, e) {
            return this.state && this.state.type && "valid" != this.state.type ? this : (this.__msg = e, 
            t && t.get_value && t.input.el && t.input.el.e ? (this.__data = t.get_value(), this.__input = t.input.el.e) : this.state = new o("invalid", 100, "??N????±???° N???N???N?"), 
            this);
        }, v.prototype.exctract = function(t) {
            return this.state && this.state.type && "valid" != this.state.type || t(this.__data) || (this.state = new o("invalid", 100, "??N????±???° N???N???N?")), 
            this;
        }, v.prototype.test_exctract_result = function(t, e) {
            return this.state && this.state.type && "valid" != this.state.type || (this.__msg = e, 
            this.__data = t), this;
        }, v.prototype.input = function(t, e) {
            return this.state && this.state.type && "valid" != this.state.type ? this : (this.__msg = e, 
            "value" in t ? (this.__data = t.value, this.__input = t) : this.state = new o("invalid", 100, "??N????±???° N???N???N?"), 
            this);
        }, v.prototype.good = function() {
            return !this.state || !this.state.type || "valid" == this.state.type;
        }, v.prototype.pipe = function(t) {
            return this.state && this.state.type && "valid" != this.state.type || t(this.__data, this.__input) || (this.state = new o("invalid", 100, "??N????±???° N???N???N?")), 
            this;
        }, v.prototype.get_input = function() {
            return this.__input;
        }, v.prototype.get_state = function() {
            return {
                state: this.state,
                msg: this.__msg
            };
        }, v.prototype.log_syslog = function(t, e) {
            return t.log(e, [ "current state", this ]), this;
        }, v.prototype.simple_handle = function() {
            return function(t) {
                if (t.good()) return !0;
                var e = t.get_state();
                return alert(e.msg + " " + b(e.state)), (t = t.get_input()) && t.focus(), !1;
            }(this);
        }, n.state_to_str = b, n.chain_valid = function(t) {
            return new v(t);
        }, n.PortFwErrorEnum = s, n.PortFwError_to_str = function(t) {
            switch (t) {
              case s.PORTFW_ERROR_IP:
                return "???µ????N?N??µ??N???N??? ?»?????°?»N???N??? IP-?°??N??µN?";

              case s.PORTFW_ERROR_SUBNET:
                return "???µ????N?N??µ??N???N??? ?»?????°?»N???N??? IP ?°??N??µN?! ??N??±?µN???N??µ N?N? ?¶?µ ??????N??µN?N?.";

              case s.PORTFW_ERROR_FROMPORT:
              case s.PORTFW_ERROR_TOPORT:
                return "???µ????N?N??µ??N???N??? ?»?????°?»N???N??? ????N?N?";

              case s.PORTFW_ERROR_EXTERNEL_FROMPORT:
              case s.PORTFW_ERROR_EXTERNEL_TOPORT:
                return "???µ????N?N??µ??N???N??? ?????µN??????? ????N?N?";

              case s.PORTFW_ERROR_RANGE:
                return "???µ????N?N??µ??N???N??? ?????°???°?·???? ?»?????°?»N???N?N? ????N?N?????";

              case s.PORTFW_EXTERNEL_RANGE:
                return "???µ????N?N??µ??N???N??? ?????°???°?·???? ?????µN??????? ????N?N?????";

              case s.PORTFW_ERROR_PROTOCOL:
                return "???µ????N?N??µ??N???N??? ??N???N????????»";

              case s.PORTFW_ERROR_EXTERNEL_IP:
                return "???µ????N?N??µ??N???N??? ?????µN??????? IP-?°??N??µN?";

              case s.PORTFW_ERROR_COMMENT:
                return "???µ????N?N??µ??N???N??? ?????????µ??N??°N?????";

              case s.PORTFW_ERROR_MAX_RULES:
                return "??N??µ??N?N??µ???? ???°??N??????°?»N??????µ ?????»??N??µN?N????? ??N??°?????»";

              case s.PORTFW_ERROR_RANGE_OVERLAP:
                return "??N??±N??°????N??? ?????°???°?·???? ????N?N????? ???µN??µ??N?N????°?µN? N?N?N??µN?N???N?N?N????µ";

              default:
                return "??N??????µN?N?N? ?????¶?°?»N???N?N??° ??N??°?????»N?????N?N?N? N???N???N? ";
            }
        };
    }, {
        ipv4: 11
    } ],
    26: [ function(t, e, n) {
        "use strict";
        function i(t, e, n) {
            return e in t ? Object.defineProperty(t, e, {
                value: n,
                enumerable: !0,
                configurable: !0,
                writable: !0
            }) : t[e] = n, t;
        }
        function s() {}
        var o = t("../../packages/nanolib/js/nano-dom.js");
        function r(t, e) {
            this.name = t, this.attr = e || {}, this.children = [], this.directives = [];
        }
        function a(t, e) {
            this.__node = new r(t, e);
        }
        function l() {
            this.hub = {};
        }
        r.prototype.child = function(t, e) {
            e = new r(t, e);
            return (e.parrent = this).children.push(e), e;
        }, r.prototype.up = function(t) {
            return this.parrent || this;
        }, r.prototype.bind = function(t) {
            return this.bind_var = t, this;
        }, r.prototype.directive = function(t, e) {
            return this.directives.push({
                name: t,
                attr: e
            }), this;
        }, r.prototype.text = function(t) {
            return this.attr.text = t, this;
        }, r.prototype.set_attr = function(t, e) {
            return this.attr[t] = e, this;
        }, r.prototype.set_class = function(t) {
            return this.className = t, this;
        }, a.prototype.root = function() {
            return this.__node;
        };
        var c = (i(t = {
            br: function(t, e) {
                var n = e.attr || {};
                this.el = o.div(), e.className && this.el.setClass(e.className);
                e = e && e.attr && e.attr.text;
                e && this.el.set(e), n.id && this.el.id(n.id);
            },
            p: function(t, e) {
                var n = e.attr || {};
                this.el = o.div(), e.className && this.el.setClass(e.className);
                e = e && e.attr && e.attr.text;
                e && this.el.set(e), n.id && this.el.id(n.id);
            }
        }, "br", function(t, e) {
            var n = e.attr || {};
            this.el = o.tag("br"), e.className && this.el.setClass(e.className);
            e = e && e.attr && e.attr.text;
            e && this.el.set(e), n.id && this.el.id(n.id);
        }), i(t, "blockquote", function(t, e) {
            e.attr;
            this.el = o.tag("blockquote");
        }), i(t, "hr", function(t, e) {
            e = e.attr || {};
            this.el = o.tag("hr"), e.size && (this.el.e.size = e.size), e.noshade && (this.el.e.noshade = e.noshade), 
            e.align;
        }), i(t, "pseudo-link", function(t, e) {
            var n = e.attr || {}, i = n.text || "";
            n.title;
            this.el = o.tag("a"), i && this.el.set(i), e.className && this.el.setClass(e.className), 
            n.id && this.el.id(n.id), n.title && (this.el.e.title = n.title);
        }), i(t, "a", function(t, e) {
            var n = e.attr || {}, i = n.text || "", s = n.href || "";
            this.el = o.link(s, i), e.className && this.el.setClass(e.className), n.id && this.el.id(n.id), 
            n.tabindex && (this.el.e.tabindex = n.tabindex);
        }), i(t, "img", function(t, e) {
            var n = e.attr || {};
            n.text, n.href;
            this.el = o.tag("img"), e.className && this.el.setClass(e.className), n.src && (this.el.e.src = n.src), 
            n.alt && (this.el.e.alt = n.alt), n.width && (this.el.e.width = n.width), n.height && (this.el.e.height = n.height), 
            n.border && (this.el.e.border = n.border), n.align, n.alt && (this.el.e.alt = n.alt);
        }), i(t, "div", function(t, e) {
            var n = e.attr || {};
            this.el = o.div(), e.className && this.el.setClass(e.className);
            e = e && e.attr && e.attr.text;
            e && this.el.set(e), n.id && this.el.id(n.id);
        }), i(t, "li", function(t, e) {
            var n = e.attr || {};
            this.el = o.tag("li"), e.className && this.el.setClass(e.className);
            e = e && e.attr && e.attr.text;
            e && this.el.set(e), n.id && this.el.id(n.id);
        }), i(t, "ul", function(t, e) {
            var n = e.attr || {};
            this.el = o.tag("ul"), e.className && this.el.setClass(e.className);
            e = e && e.attr && e.attr.text;
            e && this.el.set(e), n.id && this.el.id(n.id);
        }), i(t, "span", function(t, e) {
            e.attr;
            this.el = o.tag("span"), e.className && this.el.setClass(e.className);
            e = e && e.attr && e.attr.text;
            e && this.el.set(e);
        }), i(t, "button", function(t, e) {
            var n = e.attr || {}, i = n.text || "button", s = n.type || "button";
            this.el = o.button(i, s), e.className && this.el.setClass(e.className), n.id && this.el.id(n.id), 
            n.name && this.el.name(n.name), n.title && (this.el.e.title = n.title);
        }), i(t, "submit", function(t, e) {
            e = e && e.attr && e.attr.text;
            this.el = o.input("submit").name("submit").value(e || "button", "button");
        }), i(t, "input_text", function(t, e) {
            e = e.attr || {};
            this.el = o.input("text"), e.id && this.el.id(e.id), e.name && this.el.name(e.name), 
            e.value && this.el.value(e.value), e.autocomplete && (this.el.e.autocomplete = e.autocomplete), 
            e.placeholder && (this.el.e.placeholder = e.placeholder);
        }), i(t, "input", function(t, e) {
            var n = e.attr || {}, i = n.type || "text";
            this.el = o.input(i), n.id && this.el.id(n.id), n.name && this.el.name(n.name), 
            n.value && this.el.value(n.value), n.autocomplete && (this.el.e.autocomplete = n.autocomplete), 
            n.placeholder && (this.el.e.placeholder = n.placeholder), e.className && this.el.setClass(e.className), 
            n.maxlength && (this.el.e.maxLength = n.maxlength);
        }), i(t, "select", function(t, e) {
            e = e.attr || {};
            this.el = o.select(), e.id && this.el.id(e.id), e.name && this.el.name(e.name);
        }), i(t, "option", function(t, e) {
            e = e.attr || {};
            this.el = o.tag("option"), e.text && this.el.set(e.text), e.value && (this.el.e.value = e.value), 
            e["selected value"] && (this.el.e.value = e["selected value"], this.el.e.selected = !0);
        }), i(t, "checkbox", function(t, e) {
            e = e.attr || {};
            this.el = o.input("checkbox"), e.id && this.el.id(e.id), e.name && this.el.name(e.name);
        }), i(t, "radio", function(t, e) {
            this.el = o.input("radio");
        }), i(t, "label", function(t, e) {
            var n = e.attr || {}, i = e && e.attr && e.attr.text;
            this.el = o.label(i || ""), e.className && this.el.setClass(e.className), n.htmlFor && (this.el.e.htmlFor = n.htmlFor), 
            n.For && (this.el.e.htmlFor = n.For), n.id && this.el.id(n.id);
        }), i(t, "code", function(t, e) {
            e = e && e.attr && e.attr.text;
            this.el = o.tag("code").set(e || "");
        }), i(t, "pre", function(t, e) {
            e = e && e.attr && e.attr.text;
            this.el = o.tag("pre").set(e || "");
        }), i(t, "h2", function(t, e) {
            e = e && e.attr && e.attr.text;
            this.el = o.tag("h2").set(e || "");
        }), i(t, "h3", function(t, e) {
            e = e && e.attr && e.attr.text;
            this.el = o.tag("h3").set(e || "");
        }), i(t, "form", function(t, e) {
            var n = e.attr || {};
            this.el = o.tag("form"), n.name && this.el.name(e.attr.name), console.log(n.action), 
            n.action && (this.el.e.action = n.action), n.method && (this.el.e.method = n.method), 
            e.className && this.el.setClass(e.className), n.enctype && (this.el.e.enctype = n.enctype);
        }), t);
        function u(t) {
            this.attr = t, this.maker = f;
        }
        function d(t) {
            this.attr = t;
        }
        [ "animate", "animateTransform", "ellipse", "clipPath", "use", "svg", "defs", "circle", "path", "rect", "g", "linearGradient", "stop", "image", "polygon", "text" ].forEach(function(s) {
            c[s] = function(t, e) {
                this.el = o[s]();
                var n, i = e.attr || {};
                for (n in i) "class" != n && ("text" != n ? "id" != n ? this.el.e.setAttribute(n, i[n]) : this.el.id(i[n]) : this.el.set(i[n]));
                e.className && this.el.hattr("class", e.className);
            };
        }), d.prototype.is_render = function() {
            return this.attr;
        };
        var p = {
            "not-if": function(t) {
                return new d(!t);
            },
            if: function(t) {
                return new d(t);
            },
            repeat: function(t) {
                return new u(t);
            }
        };
        function h(t) {
            this.hub = t;
        }
        function _(e, n) {
            var t = e.hub.search(n.name);
            t || console.error("dom el: " + n.name + " invalid");
            try {
                var i = new t(e, n);
            } catch (t) {
                throw console.error("Error make " + n.name + ", Did we add it in hub?\n" + function(t) {
                    var e, n = "\n\nUser components:\n    ", i = [];
                    for (e in t.hub.hub) i.push(e);
                    for (e in n += i.join(", "), i = [], n += "\n\nInternal components:\n    ", c) i.push(e);
                    return n += i.join(", ");
                }(e) + "\n\nUse:\n     hub.registry('" + n.name + "', require('" + n.name + ".vd').Ctor);"), 
                t;
            }
            return i;
        }
        function f(t, e, n, i, s) {
            this.repeater = t.attr, this.rm = e, this.node = n, this.parrent = i, this.dir = s, 
            this.break = !1;
        }
        function m(t, e, n, i) {
            this.rm = t, this.node = e, this.parrent = n, this.dir = i, this.break = !1;
        }
        function v(t) {
            this.directives = t.map(function(t) {
                var e, e = (e = t.name, p[e]);
                if (e) return e(t.attr);
            }).filter(function(t) {
                return t;
            });
        }
        function b(t) {
            t.children.forEach(function(t) {
                b(t);
            }), t.component && (t.component.tree && b(t.component.tree), t.component.mounted && t.component.mounted());
        }
        function g(t) {
            return t.root().component;
        }
        l.prototype.registry = function(t, e) {
            var n;
            return this.hub[t] = (n = e, function(t, e) {
                e = new n(t, e);
                this.before_create = e.obj && e.obj.before_create ? e.obj.before_create : s, this.created = e.obj && e.obj.created ? e.obj.created : s, 
                this.before_mount = e.obj && e.obj.before_mount ? e.obj.before_mount : s, this.mounted = e.obj && e.obj.mounted ? e.obj.mounted : s, 
                this.before_create(), this.tree = e.tree.root(), t.make_tree(this.tree), this.created(), 
                this.el = this.tree.component.el, e.exports && (this.exports = e.exports);
            }), this;
        }, l.prototype.search = function(t) {
            var e = c[t];
            return e || this.hub[t];
        }, f.prototype.before_make = function() {
            return this.break || (this.break = !this.dir.before_make()), this;
        }, f.prototype.make = function() {
            if (this.break) return this;
            this.component = [];
            for (var t = 0; t < 1e3 && this.repeater(t, this.node); ++t) this.component.push(_(this.rm, this.node));
            return this;
        }, f.prototype.make_children = function() {
            if (this.break) return this;
            var n = this;
            return this.node.children.forEach(function(e) {
                n.component.forEach(function(t) {
                    return n.rm.make_tree(e, t);
                });
            }), this;
        }, f.prototype.mount = function() {
            return this.break || (this.node.bind_var && Object.assign(this.node.bind_var, this.component), 
            this.component.forEach(function(t) {
                t.before_mount && t.before_mount();
            })), this;
        }, f.prototype.insert = function() {
            var e = this;
            return this.break || this.component.forEach(function(t) {
                e.parrent && e.parrent.el.add(t.el);
            }), this;
        }, m.prototype.before_make = function() {
            return this.break || (this.break = !this.dir.before_make()), this;
        }, m.prototype.make = function() {
            return this.break || (this.component = _(this.rm, this.node)), this;
        }, m.prototype.make_children = function() {
            if (this.break) return this;
            var e = this;
            return this.node.children.forEach(function(t) {
                e.rm.make_tree(t, e.component);
            }), this;
        }, m.prototype.mount = function() {
            return this.break || (this.node.bind_var && Object.assign(this.node.bind_var, this.component), 
            this.component.before_mount && this.component.before_mount(), this.node.component = this.component), 
            this;
        }, m.prototype.insert = function() {
            return this.break || this.parrent && this.parrent.el.add(this.component.el), this;
        }, v.prototype.component_maker = function(t, e, n) {
            var i = this.directives.filter(function(t) {
                return t.maker;
            });
            return 0 < i.length ? new i[0].maker(i[0], t, e, n, this) : new m(t, e, n, this);
        }, v.prototype.before_make = function() {
            return !this.directives.filter(function(t) {
                return t.is_render;
            }).some(function(t) {
                return !t.is_render();
            });
        }, v.prototype.make = function(t) {}, v.prototype.mount = function(t) {}, v.prototype.insert = function(t) {}, 
        h.prototype.make_tree = function(t, e) {
            return n = this, e = e, new v((t = t).directives).component_maker(n, t, e).before_make().make().make_children().mount().insert();
            var n;
        }, h.prototype.mount_in_dom = function(t, e) {
            return e.add(t.root().component.el), b(t.root()), t;
        }, h.prototype.mount = function(t, e) {
            return o.dom(e).add(t.root().component.el), b(t.root()), t;
        }, h.prototype.remount = function(t, e) {
            return o.dom(e).set(t.root().component.el), b(t.root()), t;
        }, h.prototype.mount_front = function(t, e) {
            e = o.dom(e);
            return e.insert(t.root().component.el, e.e.firstChild), b(t.root()), t;
        }, h.prototype.mount_to_body = function(t) {
            return o.body().add(t.root().component.el), b(t.root()), t;
        }, h.prototype.render_component = function(t, e) {
            e = new a(t, e);
            return this.make_tree(e.root()), e;
        }, h.prototype.get_component = g, e.exports.VirtualDom = a, e.exports.RenderMachine = h, 
        e.exports.ComponentHub = l, e.exports.get_component = g;
    }, {
        "../../packages/nanolib/js/nano-dom.js": 138
    } ],
    27: [ function(b, t, e) {
        "use strict";
        var g = b("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            b("nano-json-rpc-2.js");
            var n = b("error-handler.js").ex_ip_2_table, i = b("dom-maker.js"), s = i.Syslog, o = i.LOG, r = (b("validations.js").chain_valid, 
            new s("ExIpFilter", {
                level: o.DEBUG
            })), a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = {}, _ = {}, f = {}, m = {}, v = {}, i = [ d, v, p, h, _, f, m ], s = {};
            this.obj = {
                created: function() {
                    r.log(o.INFO, [ "created", h ]);
                },
                mounted: function() {
                    c.exports.form.set_syslog(r), a.exports.checkbox.on("click", function() {
                        l.el.show(a.exports.checkbox.e.checked);
                    });
                }
            }, this.tree = new g("div", {}), this.tree.root().set_class("blocks-content ").child("proxy-form", {
                error_handler: function(t) {
                    r.log(o.INFO, t), t && t.code && alert("??N????±???° ??N??????µ???µ????N? N???N???N?");
                },
                submit: u,
                sub_forms: function() {
                    return [ a, c, l ];
                },
                after_update: function() {
                    p.exports.input.el.value(3), d.exports.input.el.value(0), l.el.show(a.exports.checkbox.e.checked), 
                    c.exports.form.onChange(function() {
                        return u.el.e.disabled = !1;
                    });
                }
            }).bind(s).directive("bind", s).child("h2", {}).text("? ?°N?N???N??µ????N??? IP N????»N?N?N?").up().child("single-apmib-checkbox", {
                mib: "ipFilterEnabled"
            }).bind(a).directive("bind", a).text("? ?°N?N???N??µ????N??? IP N????»N?N?N?").up().child("exctractor", {
                inputs: i,
                method: "ipFilterlist__add"
            }).bind(l).directive("bind", l).child("div", {}).set_class("blocks").child("div", {}).set_class("blocks-item blocks-item_col2").child("input-select-row", {
                opts: opts_act,
                name: "action"
            }).bind(d).directive("bind", d).text("??????").up().child("select-protocol", {}).bind(p).directive("bind", p).up().child("ip-range", {
                names: [ "sourceFirstIp", "sourceLastIp", "sourceIpMask" ]
            }).bind(h).directive("bind", h).text("???????°?»N???N??? IP ?°??N??µN?(?°)").up().child("port-range", {
                names: [ "sourceFirstPort", "sourceLastPort" ]
            }).bind(_).directive("bind", _).text("???????°?»N???N??? ????N?N?(N?)").up().up().child("div", {}).set_class("blocks-item blocks-item_col2").child("ip-range", {
                names: [ "destFirstIp", "destLastIp", "destIpMask" ]
            }).bind(f).directive("bind", f).text("???»???±?°?»N???N??? IP ?°??N??µN?(?°)").up().child("port-range", {
                names: [ "destFirstPort", "destLastPort" ]
            }).bind(m).directive("bind", m).text("???»???±?°?»N???N??? ????N?N?(N?)").up().child("input-text-row", {
                name: "comment"
            }).bind(v).directive("bind", v).text("?????????µ??N??°N?????").up().up().up().up().child("remove-list-form", {
                get_list: n,
                table_name: "ipFilterlist"
            }).bind(c).directive("bind", c).up().child("submit", {}).bind(u).directive("bind", u).text("??N??????µ????N?N?").up().up();
        };
    }, {
        "dom-maker.js": 5,
        "error-handler.js": 7,
        "nano-json-rpc-2.js": 139,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    28: [ function(i, t, e) {
        "use strict";
        var s = i("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var r = i("nano-json-rpc-2.js"), a = i("nano-dom.js"), n = i("form-widgets.js"), l = n.single_apmib_checkbox_form, c = n.FormBlock, u = n.AddRuleForm, d = n.RmListForm, p = i("validations.js").chain_valid;
            function h(t) {
                var e = t.Ipfiler_ip.e.value, n = t.Ipfiler_comment.e.value, t = parseInt(t.Ipfiler_protocol.e.value, 10);
                return r("ipFilterList_add", {
                    ip: e,
                    comment: n,
                    protoType: t
                });
            }
            function _() {
                return r("ipFilterList_get", {}).then(function(t) {
                    return {
                        header: [ "IP-?°??N??µN?", "??N???N????????»", "?????????µ??N??°N?????" ],
                        data: t.map(function(t) {
                            return [ t.ip, function(t) {
                                switch (t) {
                                  case 4:
                                    return "ICMP";

                                  case 3:
                                    return "TCP/UDP";

                                  case 2:
                                    return "UDP";

                                  case 1:
                                    return "TCP";
                                }
                                return "----";
                            }(t.protoType), t.comment ];
                        })
                    };
                });
            }
            this.obj = {
                mounted: function() {
                    var t = document.ipFilter;
                    t.submit.disabled = !0;
                    var e = l("Ipfiler_enabled", "ipFilterEnabled"), n = new u([ "Ipfiler_ip", "Ipfiler_protocol", "Ipfiler_comment" ], h);
                    n.add_validator(function() {
                        return p().input(document.ipFilter.ip, "IP ?°??N??µN?").valid("ip", {
                            required: !0
                        }).simple_handle();
                    });
                    var i = new d("ipFilterList", _, function(t) {
                        return r("ipFilterList_rm", {
                            list: t
                        });
                    }), s = new c(t.submit, [ e, i, n ]);
                    function o() {
                        return s.form_load().then(function() {
                            e.onChange(function() {
                                return n.form_disable(!e.control.e.checked);
                            }), a.dom("Ipfiler_protocol").e.value = 3, n.form_disable(!e.control.e.checked), 
                            i.onChange(function() {
                                return t.submit.disabled = !1;
                            });
                        });
                    }
                    o(), t.addEventListener("submit", function(t) {
                        t.preventDefault(), s.submit().then(o).catch(function(t) {
                            return console.log(t);
                        });
                    });
                }
            }, this.tree = new s("div", {}), this.tree.root().set_class("blocks-content").child("form", {
                action: "",
                method: "POST",
                name: "ipFilter"
            }).child("h2", {}).text("?¤???»N?N?N??°N???N? ???? IP-?°??N??µN?N?").up().child("div", {}).set_class("blocks-row blocks-row_autoWidth").child("div", {}).set_class("blocks-col blocks-leftPart").text("?¤???»N?N?N??°N???N? IP ?°??N??µN?????").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("label", {}).child("span", {}).set_class("switch switch_block-item_activate").child("input", {
                type: "checkbox",
                name: "enabled",
                id: "Ipfiler_enabled",
                value: "ON"
            }).set_class("addfield").up().child("span", {}).child("span", {}).child("span", {}).up().up().up().up().up().up().up().child("div", {}).set_class("blocks-row").child("div", {}).set_class("blocks-col blocks-leftPart").text("IP-?°??N??µN?").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("input", {
                type: "text",
                name: "ip",
                id: "Ipfiler_ip"
            }).up().up().up().child("div", {}).set_class("blocks-row").child("div", {}).set_class("blocks-col blocks-leftPart").text("??N???N????????»").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("select", {
                type: "text",
                name: "protocol",
                id: "Ipfiler_protocol"
            }).child("option", {
                value: "3"
            }).text("TCP/UDP").up().child("option", {}).text("TCP").up().child("option", {
                value: "2"
            }).text("UDP").up().child("option", {
                value: "4"
            }).text("ICMP").up().up().up().up().child("div", {}).set_class("blocks-row").child("div", {}).set_class("blocks-col blocks-leftPart").text("?????????µ??N??°N?????").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("input", {
                type: "text",
                name: "comment",
                id: "Ipfiler_comment"
            }).up().up().up().child("div", {
                id: "ipFilterList"
            }).up().child("input", {
                type: "submit",
                value: "??N??????µ????N?N?",
                name: "submit"
            }).up().up();
        };
    }, {
        "form-widgets.js": 9,
        "nano-dom.js": 138,
        "nano-json-rpc-2.js": 139,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    29: [ function(c, t, e) {
        "use strict";
        var u = c("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = c("nano-json-rpc-2.js"), i = c("form-widgets.js").AddRuleFormInputs, s = e.attr, o = (s = e.attr || {}).method_add || "", r = s.inputs_list || [], a = s.get_rpc_attr || function() {}, l = this.exports = {};
            this.obj = {
                mounted: function() {
                    console.log(r), l.form = new i(r.map(function(t) {
                        return t.exports.input.el;
                    }), function() {
                        return n(o, a(r));
                    });
                }
            }, this.tree = new u("div", {}), this.tree.root();
        };
    }, {
        "form-widgets.js": 9,
        "nano-json-rpc-2.js": 139,
        "virtual-dom.js": 26
    } ],
    30: [ function(f, t, e) {
        "use strict";
        var m = f("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = f("nano-json-rpc-2.js"), i = f("form-widgets.js").AddRuleFormInputs, s = f("error-handler.js"), o = s.Syslog, r = s.LOG, a = f("validations.js").chain_valid, l = new o("Exctractor", {
                level: r.INFO
            }), c = e.attr || {};
            var u = c.method || "", e = c.validator_res_handler, d = c.inputs || [], p = this.exports = {};
            function h() {
                this.res = {}, this.valid = !1, this.chain = [];
            }
            h.prototype.add = function(t) {
                return this.chain.push(t), this;
            }, h.prototype.validation = function(t) {
                var e = this, n = a();
                return this.chain.forEach(function(t) {
                    return t.extract(e.res, n);
                }), this.handler ? this.handler(n) : n.simple_handle();
            }, h.prototype.set_handler = function(t) {
                this.handler = t;
            }, h.prototype.validator = function(t) {
                var e = this;
                return function() {
                    return e.validation();
                };
            }, h.prototype.get_data = function() {
                return l.log(r.INFO, [ "DataExtractor get_data", this.res ]), this.res;
            }, h.prototype.reset = function(t) {
                this.valid = !1;
            };
            var _ = new h();
            e && _.set_handler(e), this.obj = {
                mounted: function() {
                    d.forEach(function(t) {
                        return _.add(t.exports.get_exctractor());
                    }), c.post_exctract && _.add(c.post_exctract), p.form = new i(d.map(function(t) {
                        return t.exports.input.el;
                    }), function() {
                        return n(u, _.get_data());
                    }), p.form.add_validator(_.validator());
                }
            }, this.tree = new m("div", {}), this.tree.root();
        };
    }, {
        "error-handler.js": 7,
        "form-widgets.js": 9,
        "nano-json-rpc-2.js": 139,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    31: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            this.tree = new i("div", {}), this.tree.root().set_class("GlobalErrorHandler").child("label", {}).up().child("span", {}).set_class("pending").up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    32: [ function(u, t, e) {
        "use strict";
        var d = u("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = u("../../packages/luna-quick-menu/js/bee-quick-lang.js").lang, i = u("event-emitter.js").EventEmiter, s = (n(), 
            e.attr), n = s.text || "", e = s.name || "", s = s.validator, o = {}, r = {}, a = this.exports = {}, l = (new i(), 
            !1);
            function c(t) {
                r.el.e.src = t ? "/password-vis.svg" : "/password-hid.svg", o.exports.change_type(t ? "text" : "password");
            }
            this.obj = {
                created: function() {
                    r.el.on("click", function(t) {
                        c(l = !l);
                    }), o.exports.change_type("password"), a.is_valid = o.exports.is_valid, a.is_changed = o.exports.is_changed, 
                    a.set_value = o.exports.set_value, a.get_value = o.exports.get_value, a.disabled = o.exports.disabled, 
                    a.changed = o.exports.changed, a.no_changed = o.exports.no_changed, a.on = o.exports.on;
                },
                mounted: function() {
                    c(l);
                }
            }, this.tree = new d("div", {}), this.tree.root().set_class("quick-label grid-password-input").child("grid-text-input", {
                validator: s,
                name: e,
                text: n,
                maxlength: "30"
            }).bind(o).directive("bind", o).up().child("img", {
                src: "/password-hid.svg"
            }).set_class("grid-password-ico").bind(r).directive("bind", r).up();
        };
    }, {
        "../../packages/luna-quick-menu/js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    33: [ function(h, t, e) {
        "use strict";
        var _ = h("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = h("../../packages/luna-quick-menu/js/bee-quick-lang.js").lang, i = h("event-emitter.js").EventEmiter, s = n(), o = e.attr, n = o.text || "", e = o.name || "";
            var r = o.validator || function(t) {
                return 0 == t.length ? (console.log("field empty"), {
                    state: !1,
                    text: s.qerror.field_empty
                }) : {
                    state: !0,
                    text: ""
                };
            }, o = o.maxlength || 63, a = {}, l = {}, c = !0, u = !1, d = this.exports = {}, p = new i();
            this.obj = {
                created: function() {
                    function e() {
                        var t = r(a.el.e.value), e = t.state, t = t.text;
                        (c = e) ? l.el.e.style.display = "none" : (l.el.set(t), l.el.e.style.display = "block");
                    }
                    d.on = function(t, e) {
                        return p.on(t, e);
                    }, d.is_valid = function() {
                        return c;
                    }, d.change_type = function(t) {
                        a.el.e.type = t;
                    }, d.is_changed = function() {
                        return u;
                    }, d.get_value = function() {
                        return a.el.e.value;
                    }, d.set_value = function(t) {
                        return a.el.e.value = t;
                    }, d.disabled = function(t) {
                        a.el.disabled(t);
                    }, l.el.e.style.display = "none", d.changed = function() {
                        u = !0, e(), p.emit("change", a.el.e.value);
                    }, d.no_changed = function() {
                        u = !1;
                    }, a.el.on("input", function(t) {
                        u = !0, e(), p.emit("change", a.el.e.value);
                    });
                }
            }, this.tree = new _("div", {}), this.tree.root().set_class("quick-label").child("div", {}).set_class("quick-label grid-input-line").child("label", {
                text: n
            }).set_class("quick-label grid-input-left").up().child("div", {}).set_class("quick-label grid-input-right").child("input", {
                maxlength: o,
                name: e,
                type: "text"
            }).set_class("grid-input-input").bind(a).directive("bind", a).up().child("div", {}).set_class("quick-label grid-input-warning").child("label", {
                text: s.qerror.field_empty
            }).set_class("quick-label").bind(l).directive("bind", l).up().up().up().up();
        };
    }, {
        "../../packages/luna-quick-menu/js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    34: [ function(a, t, e) {
        "use strict";
        var l = a("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            a("form-widgets.js").single_apmib_select_form_modern, a("nano-dom.js");
            var n = e.attr, e = n.text || "", i = n.name || "", s = this.exports = {}, o = {};
            function r(t, e) {
                this.input = t, this.name = e;
            }
            r.prototype.extract = function(t) {
                return t[this.name] = this.input.get_value(), !0;
            }, this.obj = {
                created: function() {
                    s.input = o, s.get_value = function() {
                        return o.el.e.checked;
                    }, s.set_value = function(t) {
                        o.el.e.checked = t;
                    }, s.get_exctractor = function() {
                        return new r(s, i);
                    };
                }
            }, this.tree = new l("div", {}), this.tree.root().child("div", {}).set_class("blocks-row blocks-row_autoWidth").child("div", {
                text: e
            }).set_class("blocks-col blocks-leftPart").up().child("div", {}).set_class("blocks-col blocks-rightPart blocks-col_checkboxLine").child("label", {}).child("input", {
                type: "checkbox"
            }).bind(o).directive("bind", o).up().child("span", {}).child("span", {}).up().up().up().up().up();
        };
    }, {
        "form-widgets.js": 9,
        "nano-dom.js": 138,
        "virtual-dom.js": 26
    } ],
    35: [ function(t, e, n) {
        "use strict";
        var s = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            var e = e.attr.text || "", n = this.exports = {}, i = {};
            this.obj = {
                created: function() {
                    n.input = i, n.get_value = function() {
                        return i.el.e.checked;
                    }, n.set_value = function(t) {
                        i.el.e.checked = t;
                    };
                }
            }, this.tree = new s("div", {}), this.tree.root().set_class("blocks-row blocks-row_autoWidth").child("div", {
                text: e
            }).set_class("blocks-col blocks-leftPart").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("label", {}).child("span", {}).set_class("switch switch_block-item_activate").child("input", {
                type: "checkbox"
            }).bind(i).directive("bind", i).up().child("span", {}).child("span", {}).child("span", {}).up().up().up().up().up().up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    36: [ function(o, t, e) {
        "use strict";
        var r = o("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            o("nano-dom.js");
            var n = e.attr, e = n.text || "", i = n.from || {}, s = n.to || {};
            this.obj = {
                created: function() {
                    s.exports = {
                        input: s,
                        get_value: function() {
                            return s.el.e.value;
                        }
                    }, i.exports = {
                        input: i,
                        get_value: function() {
                            return i.el.e.value;
                        }
                    };
                }
            }, this.tree = new r("div", {}), this.tree.root().set_class("blocks-row").child("div", {
                text: e
            }).set_class("blocks-col blocks-leftPart").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("input", {
                type: "text"
            }).bind(i).directive("bind", i).up().up().child("div", {}).set_class("blocks-col blocks-rightPart").child("input", {
                type: "text"
            }).bind(s).directive("bind", s).up().up();
        };
    }, {
        "nano-dom.js": 138,
        "virtual-dom.js": 26
    } ],
    37: [ function(c, t, e) {
        "use strict";
        var u = c("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            c("nano-dom.js"), c("event-emitter.js").EventEmiter;
            var n = e.attr, e = n.text || "", i = n.opts || [], s = n.name || "", o = this.exports = {}, r = {};
            function a(t, e) {
                this.input = t, this.name = e;
            }
            function l() {}
            a.prototype.extract = function(t) {
                return t[this.name] = parseInt(this.input.get_value(), 10), !0;
            }, this.obj = {
                created: function() {
                    o.on = function(t, e) {
                        return r.el.on(t, e);
                    }, r.el.addOptions(i), o.input = r, o.get_value = function() {
                        return r.el.e.value;
                    }, o.update = function(t) {
                        r.el.e.options.length = 0, r.el.addOptions(t);
                    }, o.set_value = function(t) {
                        r.el.e.value = t;
                    }, o.get_exctractor = function() {
                        return new a(o, s);
                    }, o.valid = l, o.invalid = l, o.is_valid = function() {
                        return !0;
                    };
                }
            }, this.tree = new u("div", {}), this.tree.root().set_class("blocks-row").child("div", {
                text: e
            }).set_class("blocks-col blocks-leftPart").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("select", {}).bind(r).directive("bind", r).up().up();
        };
    }, {
        "event-emitter.js": 8,
        "nano-dom.js": 138,
        "virtual-dom.js": 26
    } ],
    38: [ function(a, t, e) {
        "use strict";
        var l = a("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            a("form-widgets.js").single_apmib_select_form_modern;
            var n = e.attr, e = n.text || "", i = n.name || "", s = this.exports = {}, o = {}, n = {
                component: "label",
                attr: {
                    text: e
                }
            }, e = {
                component: "bind-input",
                attr: {
                    input: "input_text",
                    bind: o,
                    attr: {
                        name: i
                    }
                }
            };
            function r(t, e) {
                this.input = t, this.name = e;
            }
            r.prototype.extract = function(t) {
                return t[this.name] = this.input.get_value(), !0;
            }, this.obj = {
                created: function() {
                    s.input = o, s.get_value = function() {
                        return o.el.e.value;
                    }, s.set_value = function(t) {
                        o.el.e.value = t;
                    }, s.get_exctractor = function() {
                        return new r(s, i);
                    };
                }
            }, this.tree = new l("nbn-blocks-row", {
                left: n,
                right: e
            }), this.tree.root();
        };
    }, {
        "form-widgets.js": 9,
        "virtual-dom.js": 26
    } ],
    39: [ function(d, t, e) {
        "use strict";
        var p = d("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = d("error-handler.js"), i = n.Syslog, o = n.LOG, r = new i("ip-range", {
                level: o.INFO
            }), e = e.attr, a = e.text || "", s = e.names || [ "", "", "" ], e = s[0], l = {};
            function c(t, e) {
                this.input = t, this.names = e;
            }
            c.prototype.extract = function(i, t) {
                var s = this;
                return t.input_component(this.input.exports, a).exctract(function(t) {
                    if (r.log(o.INFO, [ "IpRange", t ]), 0 == t.length) return i[s.names[0]] = "0.0.0.0", 
                    i[s.names[1]] = "0.0.0.0", !(i[s.names[2]] = 0);
                    var e = /^([\d.]*)[ ]*-[ ]*([\d.]*)$/g.exec(t), n = /^([\d.]*)\/(\d*)$/g.exec(t), t = /^([\d.]*)$/g.exec(t);
                    return r.log(o.INFO, [ "IpRange", s.input, "range:", n, "ip_mask:", e, t, "ip:" ]), 
                    e ? (i[s.names[0]] = e[1], i[s.names[1]] = e[2], i[s.names[2]] = 32, !0) : n ? (i[s.names[0]] = n[1], 
                    i[s.names[1]] = n[1], i[s.names[2]] = parseInt(n[2], 10), !0) : !!t && (i[s.names[0]] = t[1], 
                    i[s.names[1]] = t[1], i[s.names[2]] = 32, !0);
                }).test_exctract_result(i[this.names[0]], a).valid("ip", {}).test_exctract_result(i[this.names[1]], a).valid("ip", {
                    required: !0
                }).test_exctract_result(i[this.names[2]], a).valid("int", {
                    required: !0
                });
            };
            var u = this.exports = {};
            this.obj = {
                created: function() {
                    u.get_exctractor = function() {
                        return new c(l, s);
                    }, u.input = l.exports.input;
                }
            }, this.tree = new p("input-text-row", {
                text: a,
                name: e
            }), this.tree.root().bind(l).directive("bind", l);
        };
    }, {
        "error-handler.js": 7,
        "virtual-dom.js": 26
    } ],
    40: [ function(l, t, e) {
        "use strict";
        var c = l("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            l("form-widgets.js").single_apmib_select_form_modern;
            var n = e.attr, s = n.text || "", i = n.name || "", o = this.exports = {}, r = {}, e = {
                component: "label",
                attr: {
                    text: s
                }
            }, n = {
                component: "bind-input",
                attr: {
                    input: "input_text",
                    bind: r,
                    attr: {
                        name: i
                    }
                }
            };
            function a(t, e) {
                this.input = t, this.name = e;
            }
            a.prototype.extract = function(n, t) {
                var i = this;
                return t.input(this.input.input.el.e, s).valid("ip", {}).pipe(function(t, e) {
                    return n[i.name] = t, !0;
                });
            }, this.obj = {
                created: function() {
                    o.input = r, o.get_value = function() {
                        return r.el.e.value;
                    }, o.set_value = function(t) {
                        r.el.e.value = t;
                    }, o.get_exctractor = function() {
                        return new a(o, i);
                    };
                }
            }, this.tree = new c("nbn-blocks-row", {
                left: e,
                right: n
            }), this.tree.root();
        };
    }, {
        "form-widgets.js": 9,
        "virtual-dom.js": 26
    } ],
    41: [ function(a, t, e) {
        "use strict";
        var l = a("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            a("nano-dom.js");
            var n = e.attr, s = n.text || "", i = n.from || {}, o = n.to || {}, e = i.name || "", n = o.name || "";
            function r(t, e) {
                this.input = t, this.name = e;
            }
            r.prototype.extract = function(n, t) {
                var i = this;
                return t.input_component(i.input, s).exctract(function(t) {
                    var e = i.input.get_value();
                    return n[i.name] = 0 == e.length ? 0 : parseInt(e, 10), !0;
                }).test_exctract_result(n[i.name], s).valid("port", {
                    zero: !0
                });
            }, this.obj = {
                created: function() {
                    o.exports = {
                        input: o,
                        get_value: function() {
                            return o.el.e.value;
                        }
                    }, o.exports.get_exctractor = function() {
                        return new r(o.exports, o.name);
                    }, i.exports = {
                        input: i,
                        get_value: function() {
                            return i.el.e.value;
                        }
                    }, i.exports.get_exctractor = function() {
                        return new r(i.exports, i.name);
                    };
                }
            }, this.tree = new l("div", {}), this.tree.root().set_class("blocks-row").child("div", {
                text: s
            }).set_class("blocks-col blocks-leftPart").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("input", {
                name: e,
                type: "text"
            }).bind(i).directive("bind", i).up().up().child("div", {}).set_class("blocks-col blocks-rightPart").child("input", {
                name: n,
                type: "text"
            }).bind(o).directive("bind", o).up().up();
        };
    }, {
        "nano-dom.js": 138,
        "virtual-dom.js": 26
    } ],
    42: [ function(d, t, e) {
        "use strict";
        var p = d("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = d("error-handler.js"), i = n.Syslog, s = n.LOG, e = (d("validations.js").chain_valid, 
            e.attr), o = e.text || "", r = e.names || [ "", "", "" ], e = r[0], a = new i("port-range[" + o + "]", {
                level: s.INFO
            }), l = {};
            function c(t, e) {
                this.input = t, this.names = e;
            }
            c.prototype.extract = function(t) {
                this.input.exports.get_value();
            }, c.prototype.extract = function(n, t) {
                var i = this;
                return t.input_component(this.input.exports, o).exctract(function(t) {
                    if (a.log(s.INFO, [ "PortRange", t ]), 0 == t.length) return n[i.names[0]] = 0, 
                    !(n[i.names[1]] = 0);
                    var e = /(\d*)[ ]*-[ ]*(\d*)/g.exec(t), t = /^([\d]*)$/g.exec(t);
                    return a.log(s.INFO, [ "PortRange", i.input, "range:", e, "port:", t ]), e ? (n[i.names[0]] = parseInt(e[1], 10), 
                    n[i.names[1]] = parseInt(e[2], 10), !0) : !!t && (n[i.names[0]] = parseInt(t[1], 10), 
                    n[i.names[1]] = parseInt(t[1], 10), !0);
                }).test_exctract_result(n[this.names[0]], o).valid("port", {
                    zero: !0
                }).log_syslog(a, s.INFO).test_exctract_result(n[this.names[1]], o).valid("port", {
                    zero: !0,
                    min: n[this.names[0]]
                });
            };
            var u = this.exports = {};
            this.obj = {
                created: function() {
                    u.get_exctractor = function() {
                        return new c(l, r);
                    }, u.input = l.exports.input;
                }
            }, this.tree = new p("input-text-row", {
                text: o,
                name: e
            }), this.tree.root().bind(l).directive("bind", l);
        };
    }, {
        "error-handler.js": 7,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    43: [ function(n, t, e) {
        "use strict";
        var d = n("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var i = n("form-widgets.js").FormBlock, s = {}, o = e.attr, r = o.submit || {}, a = o.sub_forms ? o.sub_forms() : [], l = o.after_update || function() {}, c = o.error_handler || function(t) {
                return console.log(t);
            }, u = this.exports = {};
            this.obj = {
                created: function() {},
                mounted: function() {
                    var e = new i(r.el.e, a.map(function(t) {
                        return t.exports.form;
                    }), o.pending);
                    function n() {
                        return e.form_load().then(function() {
                            return r.el.disabled(!0), !0;
                        }).then(l);
                    }
                    r.el.disabled(!0), n(), s.el.e.addEventListener("submit", function(t) {
                        t.preventDefault(), e.submit(function(t, e) {
                            return {
                                status: t,
                                msg: e
                            };
                        }).then(function(t) {
                            return t.status ? (o.after_good_submit && o.after_good_submit(), n()) : c(t.msg);
                        }).catch(c);
                    }), u.update = n;
                }
            }, this.tree = new d("form", {}), this.tree.root().bind(s).directive("bind", s);
        };
    }, {
        "form-widgets.js": 9,
        "virtual-dom.js": 26
    } ],
    44: [ function(d, t, e) {
        "use strict";
        var p = d("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = d("form-widgets.js").RmListRuleForm, i = d("nano-json-rpc-2.js"), s = {}, o = this.exports = {}, r = e.attr || {}, a = r.get_list || "", e = r.table_name || "", l = e + "__rm", c = e + "__get";
            function u() {
                return i(c, {}).then(a);
            }
            this.obj = {
                created: function() {
                    o.form = new n(s.el, u, function(t) {
                        return i(l, {
                            list: t
                        });
                    }, r.list_maker);
                }
            }, this.tree = new p("div", {}), this.tree.root().bind(s).directive("bind", s);
        };
    }, {
        "form-widgets.js": 9,
        "nano-json-rpc-2.js": 139,
        "virtual-dom.js": 26
    } ],
    45: [ function(t, e, n) {
        "use strict";
        var r = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            var n = e.attr, i = n.text || "??N???N????????»", e = n.opts || [ {
                value: 3,
                text: "TCP/UDP"
            }, {
                value: 1,
                text: "TCP"
            }, {
                value: 2,
                text: "UDP"
            }, {
                value: 4,
                text: "ICMP"
            } ], n = n.name || "protocol", s = {}, o = this.exports = {};
            this.obj = {
                created: function() {
                    o = Object.assign(o, s.exports);
                }
            }, this.tree = new r("input-select-row", {
                opts: e,
                name: n,
                text: i
            }), this.tree.root().bind(s).directive("bind", s);
        };
    }, {
        "virtual-dom.js": 26
    } ],
    46: [ function(l, t, e) {
        "use strict";
        var c = l("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = l("form-widgets.js").single_apmib_text_form, i = e.attr, e = i.text || "", s = i.mib || "", o = this.exports = {}, r = {}, a = {};
            this.obj = {
                created: function() {
                    o.form = n(r.el, s);
                },
                mounted: function() {
                    a.el.on("click", function() {
                        this.previousElementSibling.type = "password" == this.previousElementSibling.type ? "text" : "password";
                    });
                }
            }, this.tree = new c("div", {}), this.tree.root().set_class("blocks-row").child("div", {
                text: e
            }).set_class("blocks-col blocks-leftPart").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("input", {
                type: "password"
            }).set_class("latin").bind(r).directive("bind", r).up().child("span", {}).set_class("showPassword").bind(a).directive("bind", a).up().up();
        };
    }, {
        "form-widgets.js": 9,
        "virtual-dom.js": 26
    } ],
    47: [ function(c, t, e) {
        "use strict";
        var u = c("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = c("form-widgets.js").single_apmib_select_form_modern, i = e.attr, e = i.text || "", s = i.mib || "", o = i.type || "string", r = i.opts || [], a = this.exports = {}, l = {};
            this.obj = {
                created: function() {
                    a.form = n(l.el, r, s), "int" == o && (a.form.pre_applyer = function(t) {
                        return parseInt(t, 10);
                    });
                }
            }, this.tree = new u("div", {}), this.tree.root().set_class("blocks-row").child("div", {
                text: e
            }).set_class("blocks-col blocks-leftPart").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("select", {}).bind(l).directive("bind", l).up().up();
        };
    }, {
        "form-widgets.js": 9,
        "virtual-dom.js": 26
    } ],
    48: [ function(a, t, e) {
        "use strict";
        var l = a("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = a("form-widgets.js").single_apmib_text_form, i = e.attr, e = i.text || "", s = i.mib || "", o = this.exports = {}, r = {};
            this.obj = {
                created: function() {
                    o.form = n(r.el, s);
                }
            }, this.tree = new l("div", {}), this.tree.root().set_class("blocks-row").child("div", {
                text: e
            }).set_class("blocks-col blocks-leftPart").up().child("div", {}).set_class("blocks-col blocks-rightPart").child("input", {
                type: "text"
            }).bind(r).directive("bind", r).up().up();
        };
    }, {
        "form-widgets.js": 9,
        "virtual-dom.js": 26
    } ],
    49: [ function(e, t, n) {
        "use strict";
        t.exports.registry = function(t) {
            t.registry("no-login-static-text", e("../basic-components/vd/no-login-static-text.vd").Ctor), 
            t.registry("text-input", e("../basic-components/vd/text-input.vd").Ctor), t.registry("password-input", e("../basic-components/vd/password-input.vd").Ctor), 
            t.registry("modern-text-input", e("../basic-components/vd/modern-text-input.vd").Ctor), 
            t.registry("modern-selector-input", e("../basic-components/vd/modern-selector-input.vd").Ctor), 
            t.registry("login-text-input", e("../basic-components/vd/modern-text-input.vd").Ctor), 
            t.registry("login-password-input", e("../basic-components/vd/modern-password-input.vd").Ctor), 
            t.registry("submit-input", e("../basic-components/vd/submit-input.vd").Ctor);
        };
    }, {
        "../basic-components/vd/modern-password-input.vd": 50,
        "../basic-components/vd/modern-selector-input.vd": 51,
        "../basic-components/vd/modern-text-input.vd": 52,
        "../basic-components/vd/no-login-static-text.vd": 53,
        "../basic-components/vd/password-input.vd": 54,
        "../basic-components/vd/submit-input.vd": 55,
        "../basic-components/vd/text-input.vd": 56
    } ],
    50: [ function(d, t, e) {
        "use strict";
        var p = d("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = d("multilang.js").lang, i = d("event-emitter.js").EventEmiter, s = n(), o = e.attr, n = o.text || "", e = o.name || "";
            o.validator;
            var r = {}, a = {}, l = this.exports = {}, c = (new i(), !1);
            function u(t) {
                a.el.e.src = t ? "/password-vis.svg" : "/password-hid.svg", r.exports.change_type(t ? "text" : "password");
            }
            this.obj = {
                created: function() {
                    a.el.on("click", function(t) {
                        u(c = !c);
                    }), r.exports.change_type("password"), l.is_valid = r.exports.is_valid, l.is_changed = r.exports.is_changed, 
                    l.set_value = r.exports.set_value, l.get_value = r.exports.get_value, l.disabled = r.exports.disabled, 
                    l.changed = r.exports.changed, l.no_changed = r.exports.no_changed, l.on = r.exports.on;
                },
                mounted: function() {
                    u(c);
                }
            }, this.tree = new p("div", {}), this.tree.root().set_class("modern-password-input").child("modern-text-input", {
                name: e,
                text: n
            }).bind(r).directive("bind", r).up().child("img", {
                src: "/password-hid.svg"
            }).set_class("hidvis").bind(a).directive("bind", a).up();
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    51: [ function(g, t, e) {
        "use strict";
        var x = g("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = g("multilang.js").lang, i = g("system.js").$, s = g("event-emitter.js").EventEmiter, o = n(), n = e.attr, r = (n.text, 
            n.name, {}), a = {}, l = {}, c = {}, e = {}, u = {}, d = n.opts || [], p = void 0, h = !1, _ = !1, f = this.exports = {}, m = new s();
            function v(e) {
                var t = d.filter(function(t) {
                    return t.value == e;
                });
                t.length && (p = e, l.el.set(e), t[0].li && (d.forEach(function(t) {
                    return t.li.setClass("");
                }), t[0].li.setClass("selected")), r.el.setClass(""), m.emit("change", p));
            }
            function b() {
                _ = !_, c.el.show(_), r.el.setClass(_ ? "active" : "");
            }
            this.obj = {
                created: function() {
                    d.forEach(function(t) {
                        var e = i.tag("li");
                        e.set(t.value), e.id(t.name), e.on("click", function() {
                            v(t.value), b();
                        }), t.li = e, p = t.name, l.el.set(t.value), c.el.add(e);
                    }), v(p), c.el.show(!1), a.el.on("click", b), f.on = function(t, e) {
                        return m.on(t, e);
                    }, f.is_valid = function() {
                        return !0;
                    }, f.is_changed = function() {
                        return h;
                    }, f.get_value = function() {
                        return p;
                    }, f.set_value = function(t) {
                        v(t);
                    }, f.disabled = function(t) {}, u.el.e.style.display = "none", f.changed = function() {
                        h = !0, m.emit("change", p);
                    }, f.no_changed = function() {
                        h = !1;
                    };
                }
            }, this.tree = new x("div", {}), this.tree.root().set_class("modern-input modern-text-input selectic-wrapper").child("div", {}).bind(r).directive("bind", r).child("div", {}).set_class("selectic").bind(a).directive("bind", a).child("span", {}).set_class("label").bind(l).directive("bind", l).up().child("img", {
                src: "/arrow-drop-down.svg"
            }).up().up().child("div", {}).set_class("selectic-items").bind(c).directive("bind", c).child("span", {}).set_class("selectic-scroll").child("ul", {}).bind(e).directive("bind", e).up().up().up().up().child("label", {
                text: o.error.field_empty
            }).bind(u).directive("bind", u).up();
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    52: [ function(h, t, e) {
        "use strict";
        var _ = h("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = h("multilang.js").lang, i = h("event-emitter.js").EventEmiter, s = n(), o = e.attr, n = o.text || "", e = o.name || "", o = "id__" + e, r = {}, a = {}, l = {}, c = !0, u = !1, d = this.exports = {}, p = new i();
            this.obj = {
                created: function() {
                    function e() {
                        0 == r.el.e.value.length ? (l.el.e.style.display = "block", l.el.set(s.error.field_empty), 
                        a.el.setClass("form-group bad"), c = !1) : (c = !0, l.el.set(s.error.goto_configured), 
                        l.el.e.style.display = "none", a.el.setClass("form-group good"));
                    }
                    d.on = function(t, e) {
                        return p.on(t, e);
                    }, d.is_valid = function() {
                        return c;
                    }, d.change_type = function(t) {
                        r.el.e.type = t;
                    }, d.is_changed = function() {
                        return u;
                    }, d.get_value = function() {
                        return r.el.e.value;
                    }, d.set_value = function(t) {
                        return r.el.e.value = t;
                    }, d.disabled = function(t) {
                        r.el.disabled(t);
                    }, l.el.e.style.display = "none", d.changed = function() {
                        u = !0, e(), p.emit("change", r.el.e.value);
                    }, d.no_changed = function() {
                        u = !1;
                    }, r.el.on("input", function(t) {
                        u = !0, e(), p.emit("change", r.el.e.value);
                    });
                }
            }, this.tree = new _("div", {}), this.tree.root().set_class("modern-input modern-text-input").child("div", {}).set_class("form-group").bind(a).directive("bind", a).child("input", {
                id: o,
                placeholder: n,
                name: e,
                type: "text"
            }).bind(r).directive("bind", r).up().child("label", {
                htmlFor: o,
                text: n
            }).up().up().child("div", {}).set_class("modern-input-warning").child("label", {
                text: s.error.field_empty
            }).bind(l).directive("bind", l).up().up();
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    53: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("static-info.js").no_login_static_info, i = e.attr, e = i.text || "", s = i.mib || "", o = (this.exports = {}, 
            {});
            this.obj = {
                created: function() {
                    o.el.set("" + n(s));
                }
            }, this.tree = new a("li", {}), this.tree.root().set_class("info-list_item").child("label", {
                text: e
            }).set_class("info_list-text").up().child("label", {}).set_class("info_list-text").bind(o).directive("bind", o).up();
        };
    }, {
        "static-info.js": 22,
        "virtual-dom.js": 26
    } ],
    54: [ function(v, t, e) {
        "use strict";
        var b = v("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = v("multilang.js").lang, i = v("event-emitter.js").EventEmiter, s = n(), o = e.attr, n = o.text || "", e = o.name || "";
            var r = o.validator || function(t) {
                return 0 == t.length ? {
                    state: !1,
                    text: s.error.field_empty
                } : /^\w*$/.test(t) ? {
                    state: !0,
                    text: ""
                } : {
                    state: !1,
                    text: s.error.field_invalid
                };
            }, a = {}, l = {}, c = {}, u = !0, d = !1, p = this.exports = {}, h = new i();
            function _() {
                var t = r(a.el.e.value), e = t.state, t = t.text;
                (u = e) ? l.el.e.style.display = "none" : (l.el.set(t), l.el.e.style.display = "block");
            }
            var f = !1;
            function m(t) {
                c.el.e.src = t ? "/password-vis.svg" : "/password-hid.svg", a.el.e.type = t ? "text" : "password";
            }
            this.obj = {
                created: function() {
                    c.el.on("click", function(t) {
                        m(f = !f);
                    }), p.on = function(t, e) {
                        return h.on(t, e);
                    }, p.is_valid = function() {
                        return u;
                    }, p.is_changed = function() {
                        return d;
                    }, p.get_value = function() {
                        return a.el.e.value;
                    }, p.set_value = function(t) {
                        return a.el.e.value = t;
                    }, p.disabled = function(t) {
                        a.el.disabled(t);
                    }, l.el.e.style.display = "none", p.changed = function() {
                        d = !0, _(), h.emit("change", a.el.e.value);
                    }, p.no_changed = function() {
                        d = !1;
                    }, a.el.on("input", function(t) {
                        d = !0, _(), h.emit("change", a.el.e.value);
                    });
                },
                mounted: function() {
                    m(f);
                }
            }, this.tree = new b("div", {}), this.tree.root().set_class("password-input").child("div", {}).set_class("static-grid-row").child("label", {
                text: n
            }).set_class("static-grid-cell static-grid-cell1").up().child("span", {}).set_class("static-grid-cell static-grid-cell2").child("div", {}).set_class("password-input-input").child("input", {
                name: e,
                type: "password"
            }).set_class('""').bind(a).directive("bind", a).up().child("img", {
                src: "/password-hid.svg"
            }).bind(c).directive("bind", c).up().up().up().up().child("span", {}).set_class("static-grid-row").child("label", {
                text: s.error.field_empty
            }).set_class("static-grid-cell").bind(l).directive("bind", l).up().up();
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    55: [ function(t, e, n) {
        "use strict";
        var o = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            var e = e.attr.text || "", n = {}, i = {}, s = this.exports = {};
            this.obj = {
                created: function() {
                    s.disabled = function() {
                        n.el.disabled(!0);
                    }, s.enabled = function() {
                        n.el.disabled(!1);
                    }, s.pending = function(t) {
                        i.el.show(t);
                    }, s.on = function(t, e) {
                        n.el.on(t, e);
                    }, i.el.show(!1);
                }
            }, this.tree = new o("span", {}), this.tree.root().set_class("submit-input").child("input", {
                value: e,
                name: e,
                type: "submit"
            }).set_class('""').bind(n).directive("bind", n).up().child("div", {}).set_class("spinner").bind(i).directive("bind", i).child("div", {}).set_class("bounce1").up().child("div", {}).set_class("bounce2").up().child("div", {}).set_class("bounce3").up().up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    56: [ function(d, t, e) {
        "use strict";
        var p = d("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = d("multilang.js").lang, i = d("event-emitter.js").EventEmiter, s = n(), n = e.attr, e = n.text || "", n = n.name || "", o = {}, r = {}, a = !0, l = !1, c = this.exports = {}, u = new i();
            this.obj = {
                created: function() {
                    function e() {
                        0 == o.el.e.value.length ? (r.el.e.style.display = "block", r.el.set(s.error.field_empty), 
                        a = !1) : /^\w*$/.test(o.el.e.value) ? (a = !0, r.el.set(s.error.goto_configured), 
                        r.el.e.style.display = "none") : (r.el.e.style.display = "block", r.el.set(s.error.field_invalid), 
                        a = !1);
                    }
                    c.on = function(t, e) {
                        return u.on(t, e);
                    }, c.is_valid = function() {
                        return a;
                    }, c.is_changed = function() {
                        return l;
                    }, c.get_value = function() {
                        return o.el.e.value;
                    }, c.set_value = function(t) {
                        return o.el.e.value = t;
                    }, c.disabled = function(t) {
                        o.el.disabled(t);
                    }, r.el.e.style.display = "none", c.changed = function() {
                        l = !0, e(), u.emit("change", o.el.e.value);
                    }, c.no_changed = function() {
                        l = !1;
                    }, o.el.on("input", function(t) {
                        l = !0, e(), u.emit("change", o.el.e.value);
                    });
                }
            }, this.tree = new p("div", {}), this.tree.root().set_class("text-input").child("div", {}).set_class("static-grid-row").child("input", {
                name: n,
                type: "text"
            }).set_class("static-grid-cell static-grid-cell2").bind(o).directive("bind", o).up().child("label", {
                text: e
            }).set_class("static-grid-cell static-grid-cell1").up().up().child("span", {}).set_class("static-grid-row").child("label", {
                text: s.error.field_empty
            }).set_class("static-grid-cell").bind(r).directive("bind", r).up().up();
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    57: [ function(e, t, n) {
        "use strict";
        var i = e("./js/classic-menu.js").menu;
        t.exports.menu = i, t.exports.registry = function(t) {
            t.registry("app", e("app.vd").Ctor), t.registry("classic-menu-li", e("classic-menu-li.vd").Ctor), 
            t.registry("classic-menu-sub-ul", e("classic-menu-sub-ul.vd").Ctor), t.registry("header", e("header.vd").Ctor), 
            t.registry("header-user-panel", e("header-user-panel.vd").Ctor), t.registry("logout-ico", e("logout-ico.vd").Ctor), 
            t.registry("multilang", e("multilang.vd").Ctor);
        };
    }, {
        "./js/classic-menu.js": 58,
        "app.vd": 59,
        "classic-menu-li.vd": 60,
        "classic-menu-sub-ul.vd": 61,
        "header-user-panel.vd": 62,
        "header.vd": 63,
        "logout-ico.vd": 64,
        "multilang.vd": 65
    } ],
    58: [ function(t, e, n) {
        "use strict";
        function i() {
            this.top_elements = [], this.sub_elements = [], this.sub_menu = [], this.updaters = [];
        }
        i.prototype.add_top_element = function(t, e) {
            this.top_elements.push({
                name: t,
                show: e
            });
        }, i.prototype.add_sub_element = function(t, e, n) {
            this.sub_elements.push({
                name: t,
                top: e,
                show: n
            });
        }, i.prototype.add_sub_menu = function(t, e) {
            this.sub_menu.push({
                name: t,
                show: e
            });
        }, i.prototype.update = function(e) {
            this.top_elements.forEach(function(t) {
                return t.show(!1);
            }), this.sub_elements.forEach(function(t) {
                return t.show(!1);
            }), this.sub_menu.forEach(function(t) {
                return t.show(!1);
            });
            var t = this.sub_elements.find(function(t) {
                return e.match(t.name);
            });
            if (!t) return console.error("Menu: can't find elemet for", e, "do nothing"), !1;
            t.show(!0);
            var n = t.top, i = n;
            "wlan" == n && (i = window.pages_wlan_idx ? "wlan2" : "wlan5"), (t = this.top_elements.find(function(t) {
                return t.name == i;
            })) ? t.show(!0) : console.error("Menu: can't top element for", n, "do nothing"), 
            (t = this.sub_menu.find(function(t) {
                return t.name == n;
            })) ? t.show(!0) : console.error("Menu: can't sub menu for", n, "do nothing");
        };
        var s = {};
        e.exports.menu = function() {
            return s.menu || (s.menu = new i()), s.menu;
        };
    }, {} ],
    59: [ function(g, t, e) {
        !function(v) {
            "use strict";
            var b = g("virtual-dom.js").VirtualDom;
            t.exports.Ctor = function(t, e) {
                var n = g("multilang.js").lang, i = g("notify-system.js").notify_sys, n = n(), s = {}, o = {}, r = {}, a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = {}, _ = {}, f = {}, m = {};
                this.obj = {
                    mounted: function() {
                        r.el.show(!1), u.el.show(!1), d.el.show(!1), a.el.show(!1), !{
                            end: 0,
                            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                            BUILD: "debug"
                        }.CONFIG_CUSTOMER_BEELINE ? (l.el.show(!1), c.el.show(!1)) : (r.el.show(!1), a.el.show(!1), 
                        p.el.show(!1), o.el.show(!1)), i().setGlobalNotify(s), cpe().login_rpc("auth_status", {}).then(function(t) {
                            v.issuper = t.SuperUser, t.SuperUser || (h.el.show(!1), _.el.show(!1), f.el.show(!1), 
                            m.el.show(!1));
                        });
                    }
                }, this.tree = new b("div", {}), this.tree.root().set_class("app root").child("div", {}).set_class("app top").child("header", {}).up().child("ul", {}).set_class("menu top").child("classic-menu-li", {
                    text: n.menu.status,
                    topName: "status",
                    name: "status"
                }).up().child("classic-menu-li", {
                    text: n.menu.settings,
                    topName: "settings",
                    name: "settings"
                }).up().child("classic-menu-li", {
                    text: n.menu.wifi2,
                    topName: "wlan",
                    name: "wlan2"
                }).up().child("classic-menu-li", {
                    text: n.menu.wifi5,
                    topName: "wlan",
                    name: "wlan5"
                }).up().child("classic-menu-li", {
                    text: n.menu.firewall,
                    topName: "firewall",
                    name: "firewall"
                }).up().child("classic-menu-li", {
                    text: n.menu.additional,
                    topName: "additional",
                    name: "additional"
                }).up().child("classic-menu-li", {
                    text: n.menu.managment,
                    topName: "managment",
                    name: "managment"
                }).up().up().up().child("div", {}).set_class("app middle").child("global-notify", {}).bind(s).directive("bind", s).up().child("div", {}).set_class("sub-menu").child("classic-menu-sub-ul", {
                    ForTop: "status"
                }).child("classic-menu-li", {
                    text: n.menu.status,
                    ForTop: "status",
                    name: "status"
                }).up().child("classic-menu-li", {
                    text: n.menu.stats,
                    ForTop: "status",
                    name: "stats"
                }).up().child("classic-menu-li", {
                    text: n.menu.clients,
                    ForTop: "status",
                    name: "dhcptbl"
                }).up().child("classic-menu-li", {
                    text: n.menu.routes,
                    ForTop: "status",
                    name: "routetbl"
                }).up().up().child("classic-menu-sub-ul", {
                    ForTop: "settings"
                }).child("classic-menu-li", {
                    text: n.menu.wizard,
                    ForTop: "settings",
                    name: "wizard"
                }).bind(o).directive("bind", o).up().child("classic-menu-li", {
                    text: n.menu.wanlist,
                    ForTop: "settings",
                    name: "multi_wan_generic"
                }).up().child("classic-menu-li", {
                    text: n.menu.lancfg,
                    ForTop: "settings",
                    name: "lancfg"
                }).up().child("classic-menu-li", {
                    text: n.menu.bridging,
                    ForTop: "settings",
                    name: "bridging"
                }).up().child("classic-menu-li", {
                    text: n.menu.easymesh,
                    ForTop: "settings",
                    name: "multi_ap_setting_general"
                }).up().up().child("classic-menu-sub-ul", {
                    ForTop: "wlan"
                }).child("classic-menu-li", {
                    text: n.menu.wlbasic,
                    ForTop: "wlan",
                    name: "wlbasic"
                }).up().child("classic-menu-li", {
                    text: n.menu.wladvanced,
                    ForTop: "wlan",
                    name: "wladvanced"
                }).up().child("classic-menu-li", {
                    text: n.menu.wlmultipleap,
                    ForTop: "wlan",
                    name: "wlmultipleap"
                }).up().child("classic-menu-li", {
                    text: n.menu.wlsecurity,
                    ForTop: "wlan",
                    name: "wlwpa"
                }).up().child("classic-menu-li", {
                    text: n.menu.wlwds,
                    ForTop: "wlan",
                    name: "wlwds"
                }).bind(u).directive("bind", u).up().child("classic-menu-li", {
                    text: n.menu.wlactrl,
                    ForTop: "wlan",
                    name: "wlactrl"
                }).up().child("classic-menu-li", {
                    text: n.menu.wlsurvey,
                    ForTop: "wlan",
                    name: "wlsurvey"
                }).up().child("classic-menu-li", {
                    text: n.menu.wlwps,
                    ForTop: "wlan",
                    name: "wlwps"
                }).up().child("classic-menu-li", {
                    text: n.menu.wlft,
                    ForTop: "wlan",
                    name: "wlft"
                }).bind(d).directive("bind", d).up().up().child("classic-menu-sub-ul", {
                    ForTop: "firewall"
                }).child("classic-menu-li", {
                    text: n.menu.portfw,
                    ForTop: "firewall",
                    name: "portfw"
                }).up().child("classic-menu-li", {
                    text: n.menu.ipfilter,
                    ForTop: "firewall",
                    name: "fw-ipportfilter"
                }).up().child("classic-menu-li", {
                    text: n.menu.macfilter,
                    ForTop: "firewall",
                    name: "fw-macfilter"
                }).up().child("classic-menu-li", {
                    text: n.menu.whitemac,
                    ForTop: "firewall",
                    name: "whitemac"
                }).bind(a).directive("bind", a).up().child("classic-menu-li", {
                    text: n.menu.urlfilter,
                    ForTop: "firewall",
                    name: "url_blocking"
                }).up().child("classic-menu-li", {
                    text: n.menu.dos,
                    ForTop: "firewall",
                    name: "dos"
                }).bind(p).directive("bind", p).up().child("classic-menu-li", {
                    text: n.menu.dmz,
                    ForTop: "firewall",
                    name: "dmz"
                }).up().child("classic-menu-li", {
                    text: n.menu.algctl,
                    ForTop: "firewall",
                    name: "algonoff"
                }).bind(r).directive("bind", r).up().child("classic-menu-li", {
                    text: n.menu.accessctl,
                    ForTop: "firewall",
                    name: "acl"
                }).up().up().child("classic-menu-sub-ul", {
                    ForTop: "additional"
                }).child("classic-menu-li", {
                    text: n.menu.ddns,
                    ForTop: "additional",
                    name: "ddns"
                }).up().child("classic-menu-li", {
                    text: n.menu.dms,
                    ForTop: "additional",
                    name: "dms"
                }).bind(c).directive("bind", c).up().child("classic-menu-li", {
                    text: n.menu.samba,
                    ForTop: "additional",
                    name: "samba"
                }).bind(l).directive("bind", l).up().child("classic-menu-li", {
                    text: n.menu.udpxy,
                    ForTop: "additional",
                    name: "udpxy"
                }).up().child("classic-menu-li", {
                    text: n.menu.route,
                    ForTop: "additional",
                    name: "routing"
                }).up().child("classic-menu-li", {
                    text: n.menu.traffic_shaping,
                    ForTop: "additional",
                    name: "qos_traffic"
                }).bind(_).directive("bind", _).up().child("classic-menu-li", {
                    text: n.menu.qos_policy,
                    ForTop: "additional",
                    name: "qos_imq_policy"
                }).bind(f).directive("bind", f).up().child("classic-menu-li", {
                    text: n.menu.qos_classification,
                    ForTop: "additional",
                    name: "qos_cls"
                }).bind(m).directive("bind", m).up().up().child("classic-menu-sub-ul", {
                    ForTop: "managment"
                }).child("classic-menu-li", {
                    text: n.menu.saveconf,
                    ForTop: "managment",
                    name: "saveconf"
                }).up().child("classic-menu-li", {
                    text: n.menu.upgrade,
                    ForTop: "managment",
                    name: "upgrade"
                }).up().child("classic-menu-li", {
                    text: n.menu.reboot,
                    ForTop: "managment",
                    name: "reboot"
                }).up().child("classic-menu-li", {
                    text: n.menu.change_password,
                    ForTop: "managment",
                    name: "password"
                }).up().child("classic-menu-li", {
                    text: n.menu.ntp,
                    ForTop: "managment",
                    name: "tz"
                }).up().child("classic-menu-li", {
                    text: n.menu.tr069config,
                    ForTop: "managment",
                    name: "tr069config"
                }).bind(h).directive("bind", h).up().child("classic-menu-li", {
                    text: n.menu.syslog,
                    ForTop: "managment",
                    name: "syslog"
                }).up().up().up().child("div", {
                    id: "content"
                }).set_class("content").up().up();
            };
        }.call(this, "undefined" != typeof self ? self : "undefined" != typeof window ? window : {});
    }, {
        "multilang.js": 15,
        "notify-system.js": 19,
        "virtual-dom.js": 26
    } ],
    60: [ function(d, t, e) {
        "use strict";
        var p = d("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = d("multilang.js").lang, i = d("event-emitter.js").EventEmiter, s = d("navi.js").navi, o = d("classic-menu.js").menu, n = (n(), 
            e.attr), e = n.text || "", r = n.name || "", a = n.topName, l = n.ForTop, c = (this.exports = {}, 
            new i(), {});
            function u(t) {
                c.el.setClass(t ? "selected" : "");
            }
            this.obj = {
                created: function() {
                    u(!1), c.el.on("click", function(t) {
                        return s().go(r);
                    }), a ? o().add_top_element(a, u) : o().add_sub_element(r, l, u);
                },
                mounted: function() {}
            }, this.tree = new p("li", {
                id: r,
                text: e
            }), this.tree.root().bind(c).directive("bind", c);
        };
    }, {
        "classic-menu.js": 58,
        "event-emitter.js": 8,
        "multilang.js": 15,
        "navi.js": 16,
        "virtual-dom.js": 26
    } ],
    61: [ function(c, t, e) {
        "use strict";
        var u = c("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = c("multilang.js").lang, i = c("event-emitter.js").EventEmiter, s = (c("navi.js").navi, 
            c("classic-menu.js").menu), o = (n(), e.attr), n = o.text || "", e = o.name || "", r = o.ForTop, a = (this.exports = {}, 
            new i(), {});
            function l(t) {
                a.el.show(t);
            }
            this.obj = {
                created: function() {
                    a.el.show(!1), s().add_sub_menu(r, l);
                },
                mounted: function() {}
            }, this.tree = new u("ul", {
                id: e,
                text: n
            }), this.tree.root().set_class("menu sub").bind(a).directive("bind", a);
        };
    }, {
        "classic-menu.js": 58,
        "event-emitter.js": 8,
        "multilang.js": 15,
        "navi.js": 16,
        "virtual-dom.js": 26
    } ],
    62: [ function(s, t, e) {
        "use strict";
        var o = s("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = s("multilang.js").lang, i = s("event-emitter.js").EventEmiter, e = (s("navi.js").navi, 
            n(), e.attr);
            e.text, e.name, e.topName, this.exports = {}, new i();
            this.obj = {
                created: function() {},
                mounted: function() {}
            }, this.tree = new o("div", {}), this.tree.root().set_class("header-user-panel").child("multilang", {}).up().child("logout-ico", {}).up();
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "navi.js": 16,
        "virtual-dom.js": 26
    } ],
    63: [ function(s, t, e) {
        "use strict";
        var o = s("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = s("multilang.js").lang, i = s("event-emitter.js").EventEmiter, e = (s("navi.js").navi, 
            n(), e.attr);
            e.text, e.name, e.topName, this.exports = {}, new i();
            this.obj = {
                created: function() {},
                mounted: function() {}
            }, this.tree = new o("div", {}), this.tree.root().set_class("header").child("img", {
                src: "/topbar.png"
            }).up().child("header-user-panel", {}).up();
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "navi.js": 16,
        "virtual-dom.js": 26
    } ],
    64: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("multilang.js").lang, i = r("event-emitter.js").EventEmiter, s = r("navi.js").navi, e = (n(), 
            e.attr), o = (e.text, e.name, e.topName, this.exports = {}, new i(), {});
            this.obj = {
                created: function() {
                    o.el.on("click", function() {
                        document.logout.cookie_auth.value = null, s().go("login");
                    });
                },
                mounted: function() {}
            }, this.tree = new a("div", {}), this.tree.root().set_class("logout-ico").child("a", {
                href: "login.html"
            }).bind(o).directive("bind", o).child("img", {
                src: "/logout.svg"
            }).up().up();
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "navi.js": 16,
        "virtual-dom.js": 26
    } ],
    65: [ function(l, t, e) {
        "use strict";
        var c = l("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = l("multilang.js"), i = n.langOpts, s = n.langTag, n = l("event-emitter.js").EventEmiter, o = l("navi.js").navi, r = l("system.js").login_rpc, i = (e.attr, 
            this.exports = {}, new n(), i()), a = {};
            this.obj = {
                created: function() {
                    a.exports.set_value(s()), a.exports.on("change", function(t) {
                        r("tr181_set", {
                            path: "Device.UserInterface.CurrentLanguage",
                            value: t
                        }).then(function(t) {
                            o().reload("changeLang");
                        });
                    });
                },
                mounted: function() {}
            }, this.tree = new c("modern-selector-input", {
                opts: i
            }), this.tree.root().bind(a).directive("bind", a);
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "navi.js": 16,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    66: [ function(t, e, n) {
        "use strict";
        function i() {
            this._debug = !0;
        }
        i.prototype.all = function() {
            var e = this;
            return ({
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            }.CONFIG_USER_REMOTE_ACCESS_TBL ? cpe().rpc("ACL__get", {}) : Promise.resolve(!0)).then(function(t) {
                return e._debug && console.log(t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, i.prototype.add = function(t) {
            !{
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            }.CONFIG_LUNA && (t.ra_interface = parseInt(t.ra_interface, 10));
            var e = this;
            return ({
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            }.CONFIG_USER_REMOTE_ACCESS_TBL ? cpe().rpc("ACL__add", t) : Promise.resolve(!0)).then(function(t) {
                return e._debug && console.log(t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, i.prototype.rm = function(t) {
            var e = this;
            return ({
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            }.CONFIG_USER_REMOTE_ACCESS_TBL ? cpe().rpc("ACL__rm", {
                list: t
            }) : Promise.resolve(!0)).then(function(t) {
                return e._debug && console.log(t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, e.exports.ACL = i, e.exports.AclProtocols = {
            RAP_NONE: 0,
            RAP_TELNET: 1,
            RAP_SSH: 2,
            RAP_HTTP: 4,
            RAP_ICMP: 8
        };
    }, {} ],
    67: [ function(t, e, n) {
        "use strict";
        var i = t("./wifi.js").Wifi, s = t("./mib.js").Mib, o = t("./ports.js").WanPort, r = t("./multiwan.js").Multiwan, a = t("./acl.js").ACL, t = t("../../../lib/js/system.js"), l = t.rpc, t = t.login_rpc, o = {
            rpc: l,
            login_rpc: t,
            wifi: new i(),
            multiwan: new r(),
            mib: new s(),
            table: {
                ACL: new a()
            },
            ports: {
                wan: new o()
            },
            apply: function() {
                return l("apply", {});
            }
        };
        e.exports.cpe = o;
    }, {
        "../../../lib/js/system.js": 23,
        "./acl.js": 66,
        "./mib.js": 68,
        "./multiwan.js": 69,
        "./ports.js": 70,
        "./wifi.js": 71
    } ],
    68: [ function(t, e, n) {
        "use strict";
        function i() {
            this._debug = !0;
        }
        i.prototype.get = function(t) {
            var e = this;
            return cpe().rpc("rpc_apmib_get", {
                list: t
            }).then(function(t) {
                return e._debug && console.log(t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, e.exports.Mib = i;
    }, {} ],
    69: [ function(t, e, n) {
        "use strict";
        var l = t("./acl.js").AclProtocols, o = t("../../nanolib/js/os.js");
        function i() {
            var n, t;
            return n = [ "allocated", "AddressType", "vlan", "vlanid", "vlanpriority", "dnsAuto", "wanIfDns1", "wanIfDns2", "wanIfDns3", "drv_ip", "drv_mask", "drv_status", "drv_gateway", "ipv6Enable", "ipv6Addr", "ipAddr", "netMask", "gateway", "ipv6Prefix", "wanMacAddr", "pppPassword", "pppUserName", "parentWanIdx", "pppServer", "l2tp_resolved_vpn" ], 
            t = [ 1, 2, 3, 4, 5, 6, 7, 8 ].map(function(e) {
                var t = {
                    wan_idx: e,
                    list: n
                };
                return cpe().rpc("multiwan_all", t).then(function(t) {
                    return t.index = e, t;
                });
            }), Promise.all(t);
        }
        function s() {
            this._debug = !0;
        }
        function r() {
            return 1 == {
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            }.CONFIG_LUNA ? cpe().rpc("multiwan_all", {}) : i().then(function(t) {
                return t.filter(function(t) {
                    return t.allocated;
                }).map(function(t) {
                    return t.name = "WAN" + t.index, t.iface = t.name, t;
                });
            });
        }
        s.prototype.free_all = function() {
            return cpe().rpc("multiwan_free_all", {}).catch(function(t) {
                console.error(t);
            });
        }, s.prototype.alloc_wan = function(t, e, n) {
            var i = "multiwan_alloc";
            switch (t) {
              case "ipoe":
                i += "_ipoe";
                break;

              case "pppoe":
                i += "_pppoe";
                break;

              case "bridge":
                n.acl = void 0, n.isDefault = void 0, n.isTR069 = void 0, n.igmp = void 0, n.ipv6 = void 0, 
                n.isInternet = void 0, i += "_bridge";
                break;

              default:
                return console.error("unknow wan type:" + t), Promise.reject("unknown wan type:" + t);
            }
            n.isDefault && (e.isDefault = !0), n.isTR069 && (e.isTR069 = !0), n.igmp && (e.isIgmp = !0), 
            n.ipv6 && (e.ipv6 = !0), n.isInternet && (e.isInternet = !0);
            var s, o, r, a = this;
            return cpe().rpc(i, e).then(function(t) {
                return a._debug && console.log(t), t;
            }).then((s = n.acl, e = {
                ra_enable: 1,
                ra_ip: "0.0.0.0",
                ra_mask: "0.0.0.0"
            }, r = [], s && s.web && ((o = Object.assign({}, e)).ra_protocol = l.RAP_HTTP, o.ra_port = 80, 
            r.push(o)), s && s.telnet && ((o = Object.assign({}, e)).ra_protocol = l.RAP_TELNET, 
            o.ra_port = 23, r.push(o)), s && s.ping && ((o = Object.assign({}, e)).ra_protocol = l.RAP_ICMP, 
            o.ra_port = 0, r.push(o)), function(e) {
                return r.forEach(function(t) {
                    return t.ra_interface = e;
                }), Promise.all(r.map(function(t) {
                    return cpe().table.ACL.add(t);
                })).then(function() {
                    return e;
                });
            })).catch(function(t) {
                console.error(t);
            });
        }, s.prototype.alloc_ipoe = function(t, e) {
            return this.alloc_wan("ipoe", t, e);
        }, s.prototype.alloc_pppoe = function(t, e) {
            return this.alloc_wan("pppoe", t, e);
        }, s.prototype.alloc_bridge = function(t, e) {
            return this.alloc_wan("bridge", t, e);
        }, s.prototype.all = function() {
            var e = this;
            return r().then(function(t) {
                return e._debug && console.log("this is result", t), t;
            }).catch(function(t) {
                console.error(t);
            });
        };
        var a = 0, c = 1, u = 10, d = 11, p = 12, h = 20, _ = 90, f = 91, m = 92, v = 93, b = 94, g = 95, x = 99;
        s.prototype.drv_all = function() {
            var e = this;
            return cpe().rpc("multiwan_drv_all", {}).then(function(t) {
                return t.forEach(function(t) {
                    return t.status_text = function(t) {
                        switch (t) {
                          case a:
                            return "S_DISABLED";

                          case c:
                            return "S_DICSONNECTED";

                          case u:
                            return "S_CONNECTING";

                          case d:
                            return "S_IN_IDLE";

                          case p:
                            return "S_REQ_IP";

                          case h:
                            return "S_CONNECTED";

                          case _:
                            return "S_NO_AUTH";

                          case f:
                            return "S_NO_SERVER";

                          case m:
                            return "S_NO_NO_PADO";

                          case v:
                            return "S_NO_PADS";

                          case b:
                            return "S_NO_AC";

                          case g:
                            return "S_NO_IP";

                          case x:
                            return "S_ERROR";

                          default:
                            return "";
                        }
                    }(t.status);
                }), t;
            }).then(function(t) {
                return e._debug && console.log("multiwan_drv_all", t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, s.prototype.set = function(t, e, n) {
            var i = this;
            return cpe().rpc("multiwan_set", {
                iface: t,
                list: e
            }).then(function(t) {
                return i._debug && console.log("multiwan_set", t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, s.prototype.drv_default_wan = function() {
            var e = this;
            return Promise.all([ e.drv_all(), cpe().mib.get([ "default_wan_iface" ]) ]).then(function(e) {
                var t = e[0].find(function(t) {
                    return t.iface == e[1].default_wan_iface;
                });
                if (!t) throw console.log("default wan not found"), "default wan not found";
                return t;
            }).then(function(t) {
                return e._debug && console.log("default_wan", t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, e.exports.Multiwan = s, e.exports.multiwan_packet = function(t) {
            var e, n = t, i = Array.prototype.slice.call(arguments), i = (i = [].slice.call(arguments)).slice(1);
            return function() {
                return e || (e = Object.create(n.prototype), n.apply(e, i), e);
            };
        }(function(t, e, n) {
            this.packet = {};
            var i = this.packet;
            function s() {
                return i.req = e(), i.req.then(function(t) {
                    return i.data = t;
                }), i.req;
            }
            s(), o.poll(t, s), this.get_data = function(n) {
                var t = this.packet;
                return (t.result ? Promise.resolve(t.data) : t.req).then(function(t) {
                    return t.map((e = n, function(n) {
                        return e.reduce(function(t, e) {
                            return t[e] = n[e], t;
                        }, {});
                    }));
                    var e;
                });
            };
        }, 6e4, r);
    }, {
        "../../nanolib/js/os.js": 146,
        "./acl.js": 66
    } ],
    70: [ function(t, e, n) {
        "use strict";
        function i() {}
        i.prototype.status = function() {
            var e = this;
            return cpe().rpc("port_wan_status", {}).then(function(t) {
                return e._debug && console.log(t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, e.exports.WanPort = i;
    }, {} ],
    71: [ function(t, e, n) {
        "use strict";
        var o = t("../../nanolib/js/os.js");
        function i() {}
        function s() {
            var t = [ 0, 1 ].map(function(e) {
                return cpe().rpc("wlan_get", {
                    wlan_idx: e
                }).then(function(t) {
                    return t.index = e, t;
                });
            });
            return Promise.all(t);
        }
        var r = function(t) {
            var e, n = t, i = Array.prototype.slice.call(arguments), i = (i = [].slice.call(arguments)).slice(1);
            return function() {
                return e || (e = Object.create(n.prototype), n.apply(e, i), e);
            };
        }(function(t, e, n) {
            this.packet = {};
            var i = this.packet;
            function s() {
                return i.req = e(), i.req.then(function(t) {
                    return i.data = t;
                }), i.req;
            }
            s(), o.poll(t, s), this.get_data = function(n) {
                var t = this.packet;
                return (t.result ? Promise.resolve(t.data) : t.req).then(function(t) {
                    return t.map((e = n, function(n) {
                        return e.reduce(function(t, e) {
                            return t[e] = n[e], t;
                        }, {});
                    }));
                    var e;
                });
            };
        }, 1e3, s);
        i.prototype.all = function() {
            var e = this;
            return s().then(function(t) {
                return e._debug && console.log(t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, i.prototype.set = function(t, e) {
            var n = this;
            return cpe().rpc("wlan_set", {
                wlan_idx: t,
                wifi: e
            }).then(function(t) {
                return n._debug && console.log(t), t;
            }).catch(function(t) {
                console.error(t);
            });
        }, e.exports.Wifi = i, e.exports.wlan_get_data = function(t) {
            return r().get_data(t);
        };
    }, {
        "../../nanolib/js/os.js": 146
    } ],
    72: [ function(e, t, n) {
        "use strict";
        t.exports.registry = function(t) {
            t.registry("acl", e("acl.vd").Ctor);
        };
    }, {
        "acl.vd": 73
    } ],
    73: [ function(C, t, e) {
        "use strict";
        var D = C("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = C("event-emitter.js").EventEmiter, i = C("dom-maker.js"), s = app().lang(), o = {}, r = {}, a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = {}, _ = {}, f = {}, m = {}, v = [ _, f, m, d, u, l, a, _, r ], b = v;
            function g() {
                0 == b.filter(function(t) {
                    return !t.exports.is_valid();
                }).length ? x.emit("form-valid", {}) : x.emit("form-invalid", {});
            }
            var x = new n(), w = this.exports = {}, y = [ s.acl.interface, s.acl.port, s.acl.protocol, s.acl.ip, s.acl.mask ];
            var j = [];
            function k() {
                return cpe().table.ACL.all().then(function(t) {
                    t = {
                        header: y,
                        data: t.map(function(t) {
                            return [ t.ra_interface, t.ra_port, t.ra_protocol, t.ra_ip, t.ra_mask ];
                        })
                    }, t = i.render_rm_list_rostelecom(t.header, t.data, i.checkbox_rm);
                    h.el.set(t.dom), j = t.rms, c.el.disabled(!0), j.forEach(function(t) {
                        return t.rm.on("change", function() {
                            c.el.disabled(!1);
                        });
                    });
                });
            }
            function N(t) {
                return t.allocated && t.AddressType != i.AddressTypesEnum.bridge;
            }
            function I() {
                var t = (t = j.filter(function(t) {
                    return t.rm.e.checked;
                })).map(function(t) {
                    return t.rm.index;
                });
                o.exports.status_pending(s.notify.send), x.emit("rm", t);
            }
            var E = {
                RAP_NONE: 0,
                RAP_TELNET: 1,
                RAP_SSH: 2,
                RAP_HTTP: 4,
                RAP_ICMP: 8
            };
            function P() {
                var t, e = {
                    ra_interface: r.exports.get_value(),
                    ra_ip: l.exports.get_value(),
                    ra_mask: a.exports.get_value(),
                    ra_enable: 1
                }, n = [];
                _.exports.get_value() && ((t = Object.assign({}, e)).ra_protocol = E.RAP_TELNET, 
                t.ra_port = parseInt(d.exports.get_value(), 10), n.push(t)), f.exports.get_value() && ((t = Object.assign({}, e)).ra_protocol = E.RAP_HTTP, 
                t.ra_port = parseInt(u.exports.get_value(), 10), n.push(t)), m.exports.get_value() && ((t = Object.assign({}, e)).ra_protocol = E.RAP_ICMP, 
                t.ra_port = 0, n.push(t)), o.exports.status_pending(s.notify.send), x.emit("save", n);
            }
            function A() {
                p.el.disabled(!0), c.el.disabled(!0), o.exports.good(s.notify.done), setTimeout(function() {
                    o.exports.clear();
                }, 2e3);
            }
            x.on("rm", function(t) {
                cpe().table.ACL.rm(t).then(function() {
                    return cpe().apply();
                }).then(k).then(A);
            }), x.on("save", function(t) {
                Promise.all(t.map(function(t) {
                    return cpe().table.ACL.add(t);
                })).then(function() {
                    return cpe().apply();
                }).then(k).then(A);
            }), x.on("form-valid", function(t) {
                return p.el.disabled(!1);
            }), x.on("form-invalid", function(t) {
                return p.el.disabled(!0);
            }), this.obj = {
                created: function() {
                    u.exports.set_value(80), d.exports.set_value(23), cpe().multiwan.all().then(function(t) {
                        t = (t = t.filter(N)).map(function(t) {
                            return {
                                value: t.interface,
                                text: t.name
                            };
                        }), r.exports.update(t);
                    }), w.on = function(t, e) {
                        return x.on(t, e);
                    }, p.el.disabled(!0), v.forEach(function(t) {
                        return t.exports.on("change", g);
                    }), k(), p.el.on("click", P), c.el.on("click", I), x.emit("created", {});
                }
            }, this.tree = new D("blockquote", {}), this.tree.root().set_class("acl-page").child("h2", {
                text: s.acl.title
            }).set_class("title").up().child("abstract", {
                text: s.acl.description
            }).up().child("ros-error-log", {}).bind(o).directive("bind", o).up().child("div", {}).set_class("acl-info").child("input-select-row", {
                text: s.acl.interface,
                name: "interface"
            }).bind(r).directive("bind", r).up().child("input-text-row", {
                text: s.acl.ip,
                name: "ip"
            }).bind(l).directive("bind", l).up().child("input-text-row", {
                text: s.acl.mask,
                name: "mask"
            }).bind(a).directive("bind", a).up().up().child("div", {}).set_class("checkbox-flex").child("ros-checkbox-row", {
                text: s.acl.telnet
            }).bind(_).directive("bind", _).up().child("input-text-row", {
                text: s.acl.port,
                name: "telnet_port"
            }).set_class("input").bind(d).directive("bind", d).up().up().child("div", {}).set_class("checkbox-flex").child("ros-checkbox-row", {
                text: s.acl.web
            }).bind(f).directive("bind", f).up().child("input-text-row", {
                text: s.acl.port,
                name: "web_port"
            }).set_class("input").bind(u).directive("bind", u).up().up().child("ros-checkbox-row", {
                text: s.acl.ping
            }).bind(m).directive("bind", m).up().child("submit", {
                text: s.buttons.save
            }).bind(p).directive("bind", p).up().child("div", {}).bind(h).directive("bind", h).up().child("submit", {
                text: s.buttons.rm_selected
            }).bind(c).directive("bind", c).up();
        };
    }, {
        "dom-maker.js": 5,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    74: [ function(e, t, n) {
        "use strict";
        t.exports.registry = function(t) {
            t.registry("login", e("login.vd").Ctor);
        };
    }, {
        "login.vd": 75
    } ],
    75: [ function(y, t, e) {
        "use strict";
        var j = y("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = y("multilang.js").lang, i = y("event-emitter.js").EventEmiter, s = y("auth.js").flogin, o = y("system.js").login_rpc, r = y("form.js").bind_2_input, a = y("navi.js").navi, l = n(), c = {}, u = {}, d = {}, p = {}, h = {}, _ = {}, f = {}, m = {}, v = {}, b = new i(), g = (this.exports = {}, 
            [ v, m ]);
            function x(t) {
                return f.exports.pending(!1), 0 < t.failedCount ? (d.el.show(!0), p.el.show(!0), 
                _.el.show(!0), 2 < t.failedCount ? (u.el.set(l.title.failed_login), function t(e) {
                    if (e <= 0) return d.el.show(!1), p.el.show(!1), _.el.show(!1), v.el.show(!0), m.el.show(!0), 
                    f.el.show(!0), void u.el.set(l.title.login);
                    v.el.show(!1);
                    m.el.show(!1);
                    f.el.show(!1);
                    d.el.show(!0);
                    p.el.show(!0);
                    _.el.show(!0);
                    d.el.set(l.warning.countDown);
                    h.el.set(l.warning.count_time);
                    _.el.set(e);
                    setTimeout(function() {
                        t(e - 1);
                    }, 1e3);
                }(60 - t.countTime)) : (d.el.set(l.warning.try_pass_log_again), h.el.set(l.warning.count_try), 
                _.el.set(3 - t.failedCount))) : (d.el.show(!1), p.el.show(!1), _.el.show(!1)), !0;
            }
            function w() {
                0 == g.filter(function(t) {
                    return !t.exports.is_valid();
                }).length ? b.emit("form-valid", {}) : b.emit("form-invalid", {});
            }
            this.obj = {
                mounted: function() {
                    d.el.show(!1), p.el.show(!1), _.el.show(!1), f.exports.disabled(), r(v, m), f.exports.disabled(), 
                    g.forEach(function(t) {
                        return t.exports.on("change", w);
                    }), b.on("form-valid", function(t) {
                        return f.exports.enabled();
                    }), b.on("form-invalid", function(t) {
                        return f.exports.disabled();
                    }), d.el.show(!1), o("auth_status", {}).then(x), c.el.e.addEventListener("submit", function(t) {
                        var e;
                        t.preventDefault(), f.exports.disabled(), f.exports.pending(!0), e = m.exports.get_value(), 
                        t = v.exports.get_value(), o("auth_login", {
                            credit: s(e, t)
                        }).then(function(t) {
                            return document.cookie = "cookie_auth=" + t + ";path=/;", a().go("home"), !0;
                        }).catch(x).then(function(t) {
                            f.exports.pending(!1);
                        });
                    });
                }
            }, this.tree = new j("div", {}), this.tree.root().set_class("login-page").child("div", {}).set_class("header").child("div", {}).set_class("login-logo").child("img", {
                src: "/topbar.png"
            }).up().up().up().child("div", {}).set_class("content").child("form", {
                action: "",
                name: "login",
                method: "POST"
            }).set_class("login-form").bind(c).directive("bind", c).child("label", {
                text: l.title.login
            }).set_class("title").bind(u).directive("bind", u).up().child("login-text-input", {
                text: l.common.username,
                name: "Username"
            }).bind(m).directive("bind", m).up().child("login-password-input", {
                text: l.common.password,
                name: "Password"
            }).bind(v).directive("bind", v).up().child("div", {}).set_class("login-apply").child("submit-input", {
                text: l.button.login
            }).bind(f).directive("bind", f).up().up().child("div", {}).set_class("login-warning").child("label", {
                text: l.warning.try_pass_log_again
            }).bind(d).directive("bind", d).up().child("div", {}).bind(p).directive("bind", p).child("label", {
                text: l.warning.count_try
            }).bind(h).directive("bind", h).up().child("label", {}).bind(_).directive("bind", _).up().up().up().up().up();
        };
    }, {
        "auth.js": 2,
        "event-emitter.js": 8,
        "form.js": 10,
        "multilang.js": 15,
        "navi.js": 16,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    76: [ function(e, t, n) {
        "use strict";
        t.exports.registry = function(t) {
            t.registry("main", e("./vd/main.vd").Ctor), t.registry("quick-config", e("./vd/quick-config.vd").Ctor), 
            t.registry("netmap", e("./vd/netmap.vd").Ctor), t.registry("netmap-client", e("./vd/netmap-client.vd").Ctor), 
            t.registry("netmap-detail", e("./vd/netmap-detail.vd").Ctor), t.registry("netmap-popup-detail", e("./vd/netmap-popup-detail.vd").Ctor), 
            t.registry("bee-svg-gradients", e("./vd/bee-svg-gradients.vd").Ctor), t.registry("about", e("./vd/about.vd").Ctor), 
            t.registry("bee-bottom", e("./vd/bee-bottom.vd").Ctor), t.registry("bee-go-button", e("./vd/bee-go-button.vd").Ctor), 
            t.registry("bee-save-button", e("./vd/bee-save-button.vd").Ctor), t.registry("bee-line-info", e("./vd/bee-line-info.vd").Ctor), 
            t.registry("text-input", e("./vd/text-input.vd").Ctor), t.registry("grid-text-input", e("grid-text-input.vd").Ctor), 
            t.registry("grid-password-input", e("grid-password-input.vd").Ctor), t.registry("input-submit", e("./vd/input-submit.vd").Ctor), 
            t.registry("bee-input-password", e("./vd/bee-input-password.vd").Ctor), t.registry("bee-qmenu-pending", e("./vd/bee-qmenu-pending.vd").Ctor), 
            t.registry("wan-bridge", e("./vd/wan-bridge.vd").Ctor), t.registry("bee-switcher", e("./vd/bee-switcher.vd").Ctor), 
            t.registry("bee-auth", e("./vd/bee-auth.vd").Ctor), t.registry("bee-auth-bottom-composition", e("./vd/bee-auth-bottom-composition.vd").Ctor), 
            t.registry("bee-main-bottom-composition", e("./vd/bee-main-bottom-composition.vd").Ctor), 
            t.registry("bee-auth-bottom-warning", e("./vd/bee-auth-bottom-warning.vd").Ctor), 
            t.registry("bee-welcome", e("./vd/bee-welcome.vd").Ctor), t.registry("bee-title", e("./vd/bee-title.vd").Ctor), 
            t.registry("bee-welcome-bottom-composition", e("./vd/bee-welcome-bottom-composition.vd").Ctor), 
            t.registry("bee-twz-no-ip", e("./vd/bee-twz-no-ip.vd").Ctor), t.registry("bee-twz-no-wan", e("./vd/bee-twz-no-wan.vd").Ctor), 
            t.registry("bee-twz-no-conf", e("./vd/bee-twz-no-conf.vd").Ctor), t.registry("bee-twz-ip-ok", e("./vd/bee-twz-ip-ok.vd").Ctor), 
            t.registry("bee-twz-ip-fail", e("./vd/bee-twz-ip-fail.vd").Ctor), t.registry("bee-twz-nowan-bottom-composition", e("./vd/bee-twz-nowan-bottom-composition.vd").Ctor), 
            t.registry("bee-twz-nowan-bottom-warning", e("./vd/bee-twz-nowan-bottom-warning.vd").Ctor), 
            t.registry("arrow", e("./vd/arrow.vd").Ctor);
        };
    }, {
        "./vd/about.vd": 80,
        "./vd/arrow.vd": 81,
        "./vd/bee-auth-bottom-composition.vd": 82,
        "./vd/bee-auth-bottom-warning.vd": 83,
        "./vd/bee-auth.vd": 84,
        "./vd/bee-bottom.vd": 85,
        "./vd/bee-go-button.vd": 86,
        "./vd/bee-input-password.vd": 87,
        "./vd/bee-line-info.vd": 88,
        "./vd/bee-main-bottom-composition.vd": 89,
        "./vd/bee-qmenu-pending.vd": 90,
        "./vd/bee-save-button.vd": 91,
        "./vd/bee-svg-gradients.vd": 92,
        "./vd/bee-switcher.vd": 93,
        "./vd/bee-title.vd": 94,
        "./vd/bee-twz-ip-fail.vd": 95,
        "./vd/bee-twz-ip-ok.vd": 96,
        "./vd/bee-twz-no-conf.vd": 97,
        "./vd/bee-twz-no-ip.vd": 98,
        "./vd/bee-twz-no-wan.vd": 99,
        "./vd/bee-twz-nowan-bottom-composition.vd": 100,
        "./vd/bee-twz-nowan-bottom-warning.vd": 101,
        "./vd/bee-welcome-bottom-composition.vd": 102,
        "./vd/bee-welcome.vd": 103,
        "./vd/input-submit.vd": 104,
        "./vd/main.vd": 105,
        "./vd/netmap-client.vd": 106,
        "./vd/netmap-detail.vd": 107,
        "./vd/netmap-popup-detail.vd": 108,
        "./vd/netmap.vd": 109,
        "./vd/quick-config.vd": 110,
        "./vd/text-input.vd": 111,
        "./vd/wan-bridge.vd": 112,
        "grid-password-input.vd": 32,
        "grid-text-input.vd": 33
    } ],
    77: [ function(t, e, n) {
        "use strict";
        var i = (0, t("./bee-quick-lang.js").lang)(), s = t("../../../lib/js/dom-maker.js").WAN_STATUS_T;
        e.exports.wan_status_to_text = function(t) {
            switch (t) {
              case s.S_DISABLED:
              case s.S_DISCONNECTED:
                return i.cpe_status.ip_no_getted;

              case s.S_CONNECTING:
              case s.S_IN_IDLE:
              case s.S_REQ_IP:
                return i.cpe_status.connecting;

              case s.S_CONNECTED:
                return i.cpe_status.connect;

              case s.S_NO_AUTH:
                return i.cpe_status.no_auth;

              case s.S_NO_SERVER:
                return i.cpe_status.no_resolve;

              case s.S_NO_PADO:
              case s.S_NO_PADS:
              case s.S_NO_AC:
              case s.S_NO_IP:
              case s.S_ERROR:
                return i.cpe_status.ip_no_getted;

              default:
                return "";
            }
            return "";
        };
    }, {
        "../../../lib/js/dom-maker.js": 5,
        "./bee-quick-lang.js": 79
    } ],
    78: [ function(t, e, n) {
        e.exports = {
            qmenu: {
                up: "???»???¶?????µ N?N??°?»??",
                down: "??N???N?N?N???",
                text1: "???°N?????N??µ ?·???°??????N?N????? N??? Smart Box c N??°?·???µ?»?° ",
                bolder1: "??N?N?N?N??°N? ???°N?N?N????????°, ",
                text2: "N??·???°??N??µ ?? ?????????»N?N??µ????N?N? N?N?N?N?????N?N????°N? ???° ",
                bolder2: "???°N?N??µ N??µN???, ",
                text3: "???????µ?»??N??µN?N? N???N??? ?? ???????µ?? ?? ",
                bolder3: "USB-N?N?????N???N?N?,",
                text4: "???·N?N??°??N??µ ??N??µ ?????·?????¶????N?N??? ?? ",
                bolder4: "? ?°N?N???N??µ????N?N? ???°N?N?N????????°N?, ",
                text5: "?? N??·???°??N??µ N?N?N?N? ?±???»N?N??µ ",
                bolder5: "???± N?N????? N???N?N??µN??µ."
            },
            netmap: {
                up: "???°N?N??°",
                ip: "IP-?°??N??µN?",
                wifi: "Wi-Fi",
                lan: "???°?±?µ?»N?",
                down: "???µN???",
                name_dev: "?˜??N? N?N?N?N?????N?N????°",
                type_dev: "?????????»N?N??µ?????µ",
                speed: "??????N???N?N?N? ?????????»N?N??µ????N?",
                signal: "??N??????µ??N? N????????°?»?° (dBM)",
                help_title: "?˜??N???N????°N???N? ?? ???»???µ??N??µ",
                speed_100: "100 ???±??N?/N?"
            },
            qsettings: {
                up: "???°N?N?N?????",
                down: "?»?µ??????",
                home_i: "???????°N??????? ????N??µN????µN?",
                wifi: "WI-FI N??µN?N? N???N?N??µN??°",
                virtual_wifi: "????N?N??µ???°N? WI-FI N??µN?N?",
                tv: "A«?????»?°????A» ????",
                username: "??????????",
                l2tp_help: "?????µN?N? ??N? ?????¶?µN??µ ???°N?N?N?????N?N? ???°N? N???N?N??µN?  ???»N? ?????????»N?N??µ????N? ?? N??µN??? ?˜??N??µN????µN?.",
                username_help: "?????µ????N??µ ?? N?N??? ?????»?µ ???°N? ?»????????, ???±N?N????? ???? ???°N??????°?µN?N?N? N? N???N?N? 089.",
                password: "???°N????»N?",
                password_help: "?????µ????N??µ ???°N? ???°N????»N? ???»N? ????N?N?N????° ?? ?˜??N??µN????µN?.",
                status: "??N??°N?N?N?",
                status_help: "?? N?N????? ?????»?µ ??N????±N??°?¶?°?µN?N?N? N??µ??N?N????? N?N??°N?N?N? ?????????»N?N??µ????N? ?? N??µN??? ?˜??N??µN????µN?.",
                ssid: "?˜??N? N??µN???",
                wifi_help: "?????µN?N? ??N? ?????¶?µN??µ ???°N?N?N?????N?N? Wi-Fi-N??µN?N? ???°N??µ???? N???N?N??µN??°. Wi-Fi-N??µN?N? N???N?N??µN??° ??N??????»N??·N??µN?N?N? ???»N? ?????????»N?N??µ????N? ?? ?˜??N??µN????µN?N? ?????±???»N???N?N? N?N?N?N?????N?N???, N??°????N? ???°?? ????N?N??±N???, N????°N?N?N?????, ????N??µN????µN?-???»?°??N??µN? ?? ??N?.",
                vwifi_help: "?????µN?N? ??N? ?????¶?µN??µ ???°N?N?N?????N?N? ????N?N??µ??N?N? Wi-Fi-N??µN?N? ???°N??µ???? N???N?N??µN??°, ?????° ??N??¶???° ?µN??»?? ?? ???°?? ?? ????N?N???  ??N???N??»?? ??N?N??·N?N? ???»?? ?·???°??????N??µ, ?? ???°?? ???µ N???N??µN?N?N? N??????±N??°N?N? ???? ???°N????»N? ??N? ??N????????????? N??µN???. ???°?? ?¶?µ N?N??° N??µN?N? ?????µ?µN? ????N??°????N??µ?????µ ???° ??N?????N?N?????N?N? N?????N????±????N?N?N?.",
                ssid_help: "??N?????N????°??N??µ ?? ?????µ????N??µ ?? ???°???????µ ?????»?µ ???°?·???°?????µ ???°N??µ?? Wi-Fi-N??µN???, ????N? N??µN??? ?????»?¶???? ?±N?N?N? ???°????N??°???? ???° ?°?????»????N??????? N??·N????µ.",
                wifi_password_help: "??N?????N????°??N??µ ?? ?????µ????N??µ ?? ???°???????µ ?????»?µ ???°N????»N?, ????N???N?N??? ??N? ?±N????µN??µ ??N??????»N??·?????°N?N? ??N??? ?????????»N?N??µ?????? ?? ???°N??µ?? Wi-Fi-N??µN???. ???°N????»N? ?????»?¶?µ?? ?±N?N?N? ???µ ???µ???µ?µ 8 N??????????»???? ?? ???µ ?????»?¶?µ?? N??????µN??¶?°N?N? ?±N?????N? ????N????»?»??N?N?.",
                vssid_help: "??N?????N????°??N??µ ?? ?????µ????N??µ ?? ???°???????µ ?????»?µ ???°?·???°?????µ ???°N??µ?? ????N?N??µ?????? Wi-Fi-N??µN???, ????N? N??µN??? ?????»?¶???? ?±N?N?N? ???°????N??°???? ???° ?°?????»????N??????? N??·N????µ.",
                vpass_help: "??N?????N????°??N??µ ?? ?????µ????N??µ ?? ???°???????µ ?????»?µ ???°N????»N?, ????N???N?N??? ??N? ?±N????µN??µ ??N??????»N??·?????°N?N? ??N??? ?????????»N?N??µ?????? ?? ???°N??µ?? Wi-Fi-N??µN???. ??N??»?? ??N? ???µ ?·?°?????»????N??µ ?µ???? ?»N??±???µ N?N?N?N?????N?N????? ?????????»N?N??µ???????µ ?? ????N?N??µ?????? N??µN??? ?±N????µN? ?????µN?N? ????N?N?N??? ?? ????N??µN????µN?.",
                ssid_2: "?˜??N? N??µN??? 2????N? ",
                ssid_5: "?˜??N? N??µN??? 5????N? ",
                turn_on: "?????»N?N???N?N?",
                turn_off: "??N????»N?N???N?N?",
                tv_text: "??N??±?µN???N??µ LAN-????N?N?, ?? ????N???N?????N? N???N???N??µ ?????????»N?N???N?N? ????-??N???N?N??°????N?. ",
                help_l2tp: "??N??±?µN???N??µ LAN-????N?N?, ?? ????N???N?????N? N???N???N??µ ?????????»N?N???N?N? ????-??N???N?N??°????N?. ??N??±?µN???N??µ LAN-????N?N?, ?? ????N???N?????N? N???N???N??µ ?????????»N?N???N?N? ????-??N???N?N??°????N?.??N??±?µN???N??µ LAN-????N?N?, ?? ????N???N?????N? N???N???N??µ ?????????»N?N???N?N? ????-??N???N?N??°????N?.??N??±?µN???N??µ LAN-????N?N?, ?? ????N???N?????N? N???N???N??µ ?????????»N?N???N?N? ????-??N???N?N??°????N?.??N??±?µN???N??µ LAN-????N?N?, ?? ????N???N?????N? N???N???N??µ ?????????»N?N???N?N? ????-??N???N?N??°????N?.??N??±?µN???N??µ LAN-????N?N?, ?? ????N???N?????N? N???N???N??µ ?????????»N?N???N?N? ????-??N???N?N??°????N?.??N??±?µN???N??µ LAN-????N?N?, ?? ????N???N?????N? N???N???N??µ ?????????»N?N???N?N? ????-??N???N?N??°????N?.",
                help_tv: "??N??±?µN???N??µ LAN-????"
            },
            qabout: {
                up: "???·???°N?N?",
                down: "?±???»N?N??µ",
                text: "?? N?N????? N??°?·???µ?»?µ ??N? ?????¶?µN??µ N??·???°N?N? ??N?????????N?N? ????N???N????°N???N? ?? ???°N??µ?? N???N?N??µN??µ SmartBox.",
                model: "???°?·???°?????µ ???????µ?»?? ",
                mac: "MAC-?°??N??µN?",
                hw: "???µN?N???N? Hardware",
                fw: "???µN?N???N? ??N???N?????????",
                state: "????N?N???N??????µ",
                ext_ip: "?????µN??????? IP-?°??N??µN? ",
                local_ip: "???????°?»N???N??? IP-?°??N??µN?",
                gateway: "???»N??·",
                vpn_server: "VPN-N??µN????µN?",
                usb: "USB-????N?N?",
                remote_access: "?????°?»?µ????N??? ????N?N?N???",
                url: "URL:"
            },
            qauth: {
                up: "???°N?????",
                down: "N????°N??°?»?°",
                text1: "?????µ????N??µ ",
                username: "?˜??N? ?????»N??·?????°N??µ?»N?",
                password: "???°N????»N?",
                text2: " ?? ",
                text3: " ???»N? ????N?N?N????° ???° ????N??µN?N??µ??N? N???N??°???»?µ????N? N???N?N??µN??°. ??N? N??????¶?µN??µ ???°??N??? ??N? ???° ???°???»?µ?????µ, N??°N??????»???¶?µ???????? ???° N???N?N??µN??µ.",
                text4: " ???»N? ????N?N?N????° ???° N???N?N??µN? ???°???µN??°N??°??N? ???° ???°???»?µ?????µ."
            },
            welcome: {
                text_up: "??N? N??°??N?",
                text_down: "???????µN?N? ???°N?",
                text1: "?????±N??? ?????¶?°?»?????°N?N? ???° N?N?N??°????N?N? N???N??°???»?µ????N? Wi-Fi-N???N?N??µN??° A«?????»?°????A» - A«Smart BoxA»!",
                text2: "Smart Box a?? N?N??? Wi-Fi-N???N?N??µN? ???????????? ?????????»?µ????N? ?°?±?????µ??N?N?????N? N?N?N?N?????N?N??? A«?????»?°????A». ?? ????????N?N?N? Smart Box ??N? N??????¶?µN??µ ???±?µN????µN???N?N? ??N?N???????N?????N???N?N??????? ????N?N?N??? ???°N???N? N?N?N?N?????N?N??? ?? ???????°N????µ??N? ?˜??N??µN????µN?N? A«?????»?°????A» ?±?µ?· ??N?????????????, ?° N??°???¶?µ ?????????»N?N???N?N? ?? ???°N??»?°?¶???°N?N?N?N? ???????°N??????? N??µ?»?µ???????µ?????µ?? A«?????»?°????A» (??N??? ???°?»??N????? ????-??N???N?N??°??????). ",
                text3: "??N? N??°???¶?µ N??????¶?µN??µ ?????????»N?N???N?N? Flash-???°????????N??µ?»N? N??µN??µ?· USB-????N?N? ?? ???????µ?»??N?N?N?N? N???N???, ???????µ?? ?? ??N?N??????? ???°N??µN????°?»?°???? N?N??µ???? ??N??µN? N?N?N?N?????N?N??? ?????????»N?N??µ????N?N? ?? ?»?????°?»N??????? N??µN??? N???N?N??µN??°!",
                text4: "??N? ???°???µ?µ??N?N?, N?N??? Smart Box ????N?N??°????N? ???°?? N????»N????? ?????»???¶??N??µ?»N???N??µ N?????N?????!"
            },
            twz: {
                no_wan_up: "??N????????»?¶?°??",
                no_wan_down: "?????????°N?N?N?N?",
                no_wan: "?????????»N?N???N??µ ????N??µN????µN?-???°?±?µ?»N?, ????N???N?N??? ??N??????µ?»?? ???°?? ?? ?????°N?N???N?N? ???°N??? ??????N??°?¶???????? ?? ????N??µN????µN?-????N?N? ???° ?·?°?????µ?? ???°???µ?»?? N???N?N??µN??°. ??N??????µN?N?N??µ N???N?N???N??????µ ???????????°N???N??° WAN ???° ???µN??µ?????µ?? ???°???µ?»?? N???N?N??µN??°, ???? ?????»?¶?µ?? ???????°N?N?.",
                no_wan2: "??N??»?? ??N? ?????????»N?N????»?? ???°?±?µ?»N? ?? ????N??µN????µN?-????N?N? N???N?N??µN??° ?? ???????????°N???N? WAN ???µ ????N???N?, ???±N??°N???N??µN?N? ?? N??»N??¶?±N? ?????????µN??¶???? ???»???µ??N????? A«?????»?°????A».",
                no_conf_up: "???°N?N?N?????",
                no_conf_down: "???µ??N?",
                no_conf: "Smart Box ?µN??µ ???µ ???°N?N?N????µ?? ???»N? N??°?±??N?N? ?? N??µN??? ?˜??N??µN????µN?. ??N? ??N?N??°?µN??µN?N? ??N???N?N?N?N? N??°??N?, ????N???N?N??? ???µ ????N?N?N????µ?? ???· ?»?????°?»N??????? N??µN??? A«?????»?°????A», N?????N??µ?µ ??N??µ???? ??N? ?µN??µ ???µ ???°N?N?N??????»?? N???N?N??µN? ???»N? N??°?±??N?N? ?? N??µN??? ?˜??N??µN????µN?.",
                no_conf2: "???»N? ?±N?N?N?N????? ???°N?N?N????????? ???°?¶????N??µ ??????????N? A«???°N?N?N?????N?N?A».",
                ip_ok_up: "?????»N?????",
                ip_ok_down: "?????µN??µ??",
                ip_ok: "? ??N?N??µN? N?N????µN????? ?????»N?N????» IP-?°??N??µN? ?»?????°?»N??????? N??µN??? A«?????»?°????A». ??N??»?? ??N? ???°N?N?N??°?????°?»?? N???N?N??µN? N??°???µ?µ, ????N??µN????µN?-N????µ???????µ?????µ ?????»?¶???? N?N?N??°????????N?N?N?N? ?°??N??????°N???N??µN?????, ?µN??»?? ???µN?, ???°?¶????N??µ ??????????N? A«???°N?N?N?????N?N?A» ???»N? ???µN??µN????¶?°???° N?N?N??°????N?N? N? ???°N?N?N????????°???? ????N??µN????µN?-N????µ???????µ????N?.",
                ip_fail: "? ??N?N??µN? ???µ N??????? ?????»N?N???N?N? IP-?°??N??µN?. ??N??????µN?N?N??µ N??µ?»??N?N?????N?N?N? ???°?±?µ?»N? ?????????»N?N??µ?????????? ?? ????N??µN????µN?-????N?N? ???° ?·?°?????µ?? ???°???µ?»?? N???N?N??µN??°, ??N???N?N??µ ?? ??N?N??°??N?N??µ ?µ???? ???±N??°N?????.",
                ip_fail2: "???°?¶????N??µ ??????????N? A«???µN??µ?·?°??N?N??·??N?N?A», ?µN??»?? ????N??»?µ ???µN??µ?·?°??N?N??·???? ??N????±???° N???N?N??°??N??µN?N?N?, ???±N??°N???N??µN?N? ?? N??»N??¶?±N? ?????????µN??¶???? ???»???µ??N????? A«?????»?°????A».",
                no_ip_up: "??N????±???? ???????????°",
                no_ip_down: "N??»N?N??°N?N?N?N?",
                no_ip: "???? ???µ???·???µN?N???N??? ??N???N??????°?? N???N?N??µN? ???µ N??????? ?????»N?N???N?N? IP-?°??N??µN? ?? ?»?????°?»N??????? N??µN??? A«?????»?°????A».",
                no_ip2: "???±?µ????N??µN?N? ?? N??µ?»??N?N?????N?N??? ????N??µN????µN?-???°?±?µ?»N?, ????N???N?N??? ??N??????µ?»?? ?? ???°?? ?? ?????°N?N???N?N? ???°N??? ??????N??°?¶????????, ???? ???µ ?????»?¶?µ?? ?????µN?N? ??????????N?N? ??????N??µ?¶???µ??????.",
                no_ip3: "?§N????±N? ??N???N??°????N?N? ??N????±??N? ???°?¶????N??µ ??????????N? A«?????»N?N???N?N? IP-?°??N??µN?A» ?? ?????????¶????N??µ ???°N?N? ??????N?N?, ???????° N???N?N??µN? ??N?N??°?µN?N?N? ?????»N?N???N?N? IP-?°??N??µN? ?·?°????????.",
                no_ip4: "?? N??»?°N??°?µ ?µN??»?? ??N????±???° N???N?N??°??N??µN?N?N? ???±N??°N???N??µN?N? ?? N??»N??¶?±N? ?????????µN??¶???? A«?????»?°????A»."
            },
            button: {
                quick: "??N?N?N?N??°N? ???°N?N?N????????°",
                netmap: "???°N?N??° N??µN???",
                detail_menu: "? ?°N?N???N??µ????N??µ ???°N?N?N?????????",
                USB: "USB-N?N?????N?????",
                about: "???± N?N????? N???N?N??µN??µ",
                back: "???°?·?°??",
                save: "????N?N??°????N?N?",
                reboot: "???µN??µ?·?°??N?N??·??N?N?",
                get_ip: "?????»N?N???N?N? IP-?°??N??µN?",
                next: "??N????????»?¶??N?N?",
                conf: "???°N?N?N?????N?N?",
                on: "?????»N?N???N?N?",
                on1: "? ?°?·N??µN???N?N?",
                off: "??N????»N?N???N?N?",
                off1: "???°??N??µN???N?N?",
                main_menu: "???»?°???????µ ???µ??N?"
            },
            error: {
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                goto_configured: ". ???µN??µ??????N??µ ?? N??°?·???µ?» ?±N?N?N?N??°N? ???°N?N?N????????°",
                field_empty: "???±N??·?°N??µ?»N??????µ ?????»?µ",
                pass_less_8: "???°N????»N? N??»??N??????? ????N???N???????",
                field_invalid: "???µ????N?N??µ??N???N??? ????????"
            },
            qerror: {
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                goto_configured: ". ???µN??µ??????N??µ ?? N??°?·???µ?» ?±N?N?N?N??°N? ???°N?N?N????????°",
                field_empty: "???±N??·?°N??µ?»N??????µ ?????»?µ",
                pass_less_8: "???°N????»N? N??»??N??????? ????N???N???????",
                field_invalid: "???µ????N?N??µ??N???N??? ????????"
            },
            cpe_status: {
                connect: "?˜??N??µN????µN? ?????????»N?N??µ??",
                not_connected: "???µN? N????µ???????µ????N?",
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                router_getting_ip: "? ??N?N??µN? ?????»N?N??°?µN? IP-?°??N??µN?",
                connecting: "??N?N??°???°???»?????°?µN?N?N? ????N??µN????µN?-N????µ???????µ?????µ",
                no_wan: "???°?±?µ?»N? ???µ ?????????»N?N??µ??",
                ip_no_getted: "IP-?°??N??µN? ???µ ?????»N?N??µ??",
                no_auth: "???µ???µN??????µ ????N? ?????»N??·?????°N??µ?»N? ???»?? ???°N????»N?",
                no_resolve: "???µ N????°?µN?N?N? N??°?·N??µN???N?N? ????N? vpn-N??µN????µN??°"
            },
            warning: {
                no_configured: "Smart Box ?µN??µ ???µ ???°N?N?N????µ??",
                goto_configured: ". ???µN??µ??????N??µ ?? N??°?·???µ?» ?±N?N?N?N??°N? ???°N?N?N????????°",
                try_pass_log_again: "?????·?????¶???? ??N? ??N????±?»??N?N? ??N??? ?????????µ ?????µ???? ?????»N??·?????°N??µ?»N? ???»?? ???°N????»N?.",
                no_wan: "?˜??N??µN????µN?-???°?±?µ?»N? ?????»?¶?µ?? ?±N?N?N? ?????????»N?N??µ?? ?? N?N???N? ????N?N?.",
                coutnDown: "??N??µ??N?N??µ???? ?????»??N??µN?N????? ??????N?N?????! ??N?N??°?»??N?N? N??µ??N?????: "
            },
            pending: {
                applying: "?????????¶????N??µ, ???°N?N?N????????? N???N?N??°??N?N?N?N?N?"
            }
        };
    }, {} ],
    79: [ function(t, e, n) {
        "use strict";
        var i = t("./bee-quick-lang-ru.json");
        n.lang = function() {
            return i;
        };
    }, {
        "./bee-quick-lang-ru.json": 78
    } ],
    80: [ function(x, t, e) {
        "use strict";
        var w = x("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = (0, x("data_utility.js").get_capabilities)(), i = x("dom-maker.js"), i = (i.AddressTypesEnum, 
            i.status_to_str, x("navi.js").navi, x("system.js")), s = i.rpc, o = (i.poll, x("../js/bee-custom.js").wan_status_to_text, 
            x("twz.js").twz, x("../../cpe/js/multiwan.js").multiwan_packet), r = (n.usb_enabled, 
            (0, x("../js/bee-quick-lang.js").lang)()), a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = {}, _ = {}, f = {}, m = {}, v = {}, b = {}, g = [ "allocated", "AddressType", "vlan", "vlanid", "vlanpriority", "dnsAuto", "wanIfDns1", "wanIfDns2", "wanIfDns3", "drv_ip", "drv_mask", "ipv6Enable", "wanMacAddr", "ipv6Addr", "gateway", "ipv6Prefix", "drv_status", "pppPassword", "pppUserName", "wanType", "iface", "appType", "cwmpInform", "cwmpStatus", "dnsTotal", "drv_gateway", "ifindex", "ipAddr", "ipVersion", "l2tp_resolved_vpn", "login", "name", "netMask", "parentWanIdx", "password", "portMap", "pppServer", "vlan", "wanIfDnsList", "wanStatus", "isDefault" ];
            this.obj = {
                created: function() {
                    v.el.show(!1), s("multiwan_acl_status_get", {}).then(function(t) {
                        t.enabled && t.allocated ? (m.exports.set_value(1), console.log("enabled")) : (m.exports.set_value(0), 
                        console.log("disabled"));
                    }), s("multiwan_acl_status_get", {}).then(function(t) {
                        t.allocated && !t.enabled ? m.exports.on("change", function(t) {
                            b.exports.run(), Promise.all([ s("multiwan_acl_status_set", {
                                acl_enable: 1
                            }) ]).catch(function(t) {
                                return console.log(t);
                            }).then(function() {
                                return m.exports.set_value(1);
                            }).then(function() {
                                return b.exports.stop();
                            });
                        }) : t.allocated && t.enabled ? (console.log("acl exists and ENABLED"), m.exports.on("change", function(t) {
                            b.exports.run(), Promise.all([ s("multiwan_acl_status_set", {
                                acl_enable: 0
                            }) ]).catch(function(t) {
                                return console.log(t);
                            }).then(function() {
                                return m.exports.set_value(0);
                            }).then(function() {
                                return b.exports.stop();
                            });
                        })) : m.exports.on("change", function(t) {
                            b.exports.run(), Promise.all([ s("multiwan_add_acl", {
                                ip: "0.0.0.0",
                                mask: "0.0.0.0",
                                iface_type: 2,
                                service: 4,
                                enabled: 1
                            }).then(function() {
                                return s("multiwan_acl_status_get", {}).then(function(t) {
                                    t.enabled && m.exports.set_value(0);
                                });
                            }) ]).catch(function(t) {
                                return console.log(t);
                            }).then(function() {
                                return b.exports.stop();
                            });
                        });
                    }), s("rpc_apmib_get", {
                        list: [ "hw_version", "fw_version", "model_name" ]
                    }).then(function(t) {
                        l.exports.set_value(t.model_name), a.exports.set_value(t.fw_version), c.exports.set_value(t.hw_version);
                    }), o().get_data(g).then(function(t) {
                        var e = t.filter(function(t) {
                            return t.allocated && t.isDefault;
                        }), n = t.filter(function(t) {
                            return t.allocated && t.isDefault && "L2TP" == t.wanType;
                        });
                        1 == n.length ? (t = t.filter(function(t) {
                            return t.allocated && "IPOE" == t.wanType;
                        }), p.exports.set_value(n[0].wanMacAddr), u.exports.set_value(n[0].drv_ip), d.exports.set_value(t[0].drv_ip), 
                        _.exports.set_value(t[0].gateway || "0.0.0.0"), "" == n.l2tp_resolved_vpn ? h.exports.set_value(n[0].pppServer) : h.exports.set_value(n[0].l2tp_resolved_vpn)) : (u.el.show(!1), 
                        h.el.show(!1), d.exports.set_text("IP ?°??N??µN?"), 0 !== e.length ? (d.exports.set_value(e[0].drv_ip), 
                        _.exports.set_value(e[0].gateway), p.exports.set_value(e[0].wanMacAddr)) : (console.log(e), 
                        u.exports.set_value("..."), d.exports.set_value("..."), _.exports.set_value("..."), 
                        h.exports.set_value("tp.internet.beeline.ru"), p.exports.set_value("..."), f.exports.set_value(r.cpe_status.not_connected))), 
                        0 !== e.length ? e[0].wanStatus ? f.exports.set_value(r.cpe_status.connect) : (f.exports.set_value(r.cpe_status.not_connected), 
                        f.el.setClass("fail")) : f.exports.set_value(r.cpe_status.not_connected);
                    });
                }
            }, this.tree = new w("div", {}), this.tree.root().set_class("quick-label bee-about").child("bee-qmenu-pending", {}).bind(b).directive("bind", b).up().child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: r.qabout.up,
                down: r.qabout.down
            }).up().child("p", {}).set_class("bee-quick-margin-left bee-quick-main-text").child("label", {
                text: r.qabout.text
            }).set_class("quick-label").up().up().child("div", {}).set_class("quick-label bee-about-list").child("bee-line-info", {
                text: r.qabout.model,
                default: "..."
            }).bind(l).directive("bind", l).up().child("bee-line-info", {
                text: r.qabout.mac,
                default: "..."
            }).bind(p).directive("bind", p).up().child("bee-line-info", {
                text: r.qabout.hw,
                default: "..."
            }).bind(c).directive("bind", c).up().child("bee-line-info", {
                text: r.qabout.fw,
                default: "..."
            }).bind(a).directive("bind", a).up().child("bee-line-info", {
                text: r.qabout.state,
                default: "..."
            }).bind(f).directive("bind", f).up().child("bee-line-info", {
                text: r.qabout.vpn_server,
                default: "tp.internet.beeline.ru"
            }).bind(h).directive("bind", h).up().child("bee-line-info", {
                text: r.qabout.ext_ip,
                default: "..."
            }).bind(u).directive("bind", u).up().child("bee-line-info", {
                text: r.qabout.local_ip,
                default: "..."
            }).bind(d).directive("bind", d).up().child("bee-line-info", {
                text: r.qabout.gateway,
                default: "..."
            }).bind(_).directive("bind", _).up().child("div", {}).set_class("quick-label bee-line-info").child("div", {}).set_class("quick-label bee-line-info-name").child("label", {
                text: r.qabout.remote_access
            }).set_class("quick-label").up().up().child("div", {}).set_class("quick-label bee-line-info-value").child("bee-switcher", {
                text_on: r.button.on1,
                text_off: r.button.off1
            }).bind(m).directive("bind", m).up().up().up().child("div", {}).set_class("quick-label bee-about-url").child("bee-line-info", {
                text: r.qabout.url,
                default: "http://0.0.0.0:8080"
            }).bind(v).directive("bind", v).up().up().up().child("div", {}).set_class("bee-quick-main-1-line").child("bee-go-button", {
                text: r.button.main_menu,
                url: "main"
            }).up().up().up().child("div", {
                text: r.warning.no_configured
            }).set_class("bee-quick-bottom-warning").directive("if", !1).child("img", {
                src: "help_ico2.png"
            }).set_class("bee-ico-info").up().child("span", {}).set_class("quick-label bee-quick-bottom-text").child("span", {
                text: r.warning.no_configured
            }).set_class("text-bold").up().child("span", {
                text: r.warning.goto_configured
            }).set_class("text-bold").up().up().up().child("div", {}).set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-bottom-block").child("div", {}).set_class("quick-label bee-bottom-img").child("bee-main-bottom-composition", {}).up().up().up().up();
        };
    }, {
        "../../cpe/js/multiwan.js": 69,
        "../js/bee-custom.js": 77,
        "../js/bee-quick-lang.js": 79,
        "data_utility.js": 4,
        "dom-maker.js": 5,
        "navi.js": 16,
        "system.js": 23,
        "twz.js": 24,
        "virtual-dom.js": 26
    } ],
    81: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            this.tree = new i("svg", {
                version: "1.1",
                viewBox: "0 0 86.494 53.179",
                xmlns: "http://www.w3.org/2000/svg"
            }), this.tree.root().child("bee-svg-gradients", {}).up().child("g", {
                transform: "translate(-22.936 -93.09)"
            }).child("path", {
                fill: "#f7a600",
                d: "m80.966 137.69v-8.5759h-58.03l0.08186-2.8397c0.04502-1.5618 0.11296-4.8844 0.15097-7.3833s0.1049-5.8076 0.14866-7.3524l0.07955-2.8089 57.435 0.1032 8e-3 -1.9868c0.0171-4.1084 0.22169-13.687 0.29384-13.759 0.0866-0.08665 28.296 25.718 28.296 25.884 0 0.0856-26.578 25.591-28.106 26.971l-0.35853 0.32397z"
            }).up().up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    82: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            this.tree = new i("div", {}), this.tree.root().set_class("quick-label bee-auth-composition").child("img", {
                src: "cpe_back4.png"
            }).set_class("cpe-back").up().child("img", {
                src: "cpe_front2.png"
            }).set_class("cpe-front").up().child("div", {}).set_class("quick-label warning").child("bee-auth-bottom-warning", {}).up().up().child("div", {}).set_class("quick-label arrow").child("arrow", {}).up().up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    83: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("../js/bee-quick-lang.js").lang, i = r("event-emitter.js").EventEmiter, n = n(), s = (e.attr, 
            new i()), o = this.exports = {};
            this.obj = {
                created: function() {
                    o.on = function(t, e) {
                        return s.on(t, e);
                    };
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("quick-label bee-auth-buttom-warning").child("img", {
                src: "help_ico2.png"
            }).up().child("span", {}).set_class("bee-auth-bottom-warning-text").child("span", {
                text: n.qauth.username
            }).set_class("quick-label text-bold").up().child("label", {
                text: n.qauth.text2
            }).set_class("quick-label").up().child("span", {
                text: n.qauth.password
            }).set_class("quick-label text-bold").up().child("label", {
                text: n.qauth.text4
            }).set_class("quick-label").up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    84: [ function(g, t, e) {
        "use strict";
        var x = g("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = g("../js/bee-quick-lang.js").lang, i = g("event-emitter.js").EventEmiter, s = g("auth.js").flogin, o = (g("system.js").ajax, 
            g("system.js").login_rpc), r = g("form.js").bind_2_input, a = g("navi.js").navi, l = n(), c = {}, u = {}, d = {}, p = {}, h = {}, _ = {}, f = (e.attr, 
            new i());
            this.exports = {};
            function m(t) {
                return d.exports.pending(!1), 0 < t.failedCount && (p.el.show(!0), 2 < t.failedCount) ? _.exports.countDown(l.warning.coutnDown, 60 - t.countTime, function(t) {
                    return p.el.show(!1);
                }) : _.exports.stop(), !0;
            }
            window.login_rpc = o;
            var v = [ u, c ], f = new i();
            function b() {
                0 == v.filter(function(t) {
                    return !t.exports.is_valid();
                }).length ? f.emit("form-valid", {}) : f.emit("form-invalid", {});
            }
            this.obj = {
                created: function() {
                    r(u, c), d.exports.disabled(), v.forEach(function(t) {
                        return t.exports.on("change", b);
                    }), f.on("form-valid", function(t) {
                        return d.exports.enabled();
                    }), f.on("form-invalid", function(t) {
                        return d.exports.disabled();
                    }), p.el.show(!1), o("auth_status", {}).then(m), h.el.e.addEventListener("submit", function(t) {
                        var e;
                        t.preventDefault(), d.exports.disabled(), d.exports.pending(!0), e = c.exports.get_value(), 
                        t = u.exports.get_value(), o("auth_login", {
                            credit: s(e, t)
                        }).then(function(t) {
                            return document.cookie = "cookie_auth=" + t + ";path=/", a().go("main"), !0;
                        }).catch(m).then(function(t) {
                            d.exports.pending(!1);
                        });
                    });
                }
            }, this.tree = new x("div", {}), this.tree.root().set_class("quick-label bee-auth").child("bee-qmenu-pending", {}).bind(_).directive("bind", _).up().child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: l.qauth.up,
                down: l.qauth.down
            }).up().child("div", {}).set_class("quick-label bee-qconf-content").child("p", {}).set_class("bee-quick-main-text").child("label", {
                text: l.qauth.text1
            }).set_class("quick-label").up().child("span", {
                text: l.qauth.username
            }).set_class("quick-label text-bold").up().child("label", {
                text: l.qauth.text2
            }).set_class("quick-label").up().child("span", {
                text: l.qauth.password
            }).set_class("quick-label text-bold").up().child("label", {
                text: l.qauth.text3
            }).set_class("quick-label").up().up().child("form", {
                action: "",
                name: "login",
                method: "POST"
            }).bind(h).directive("bind", h).child("div", {}).set_class("quick-label bee-qconf-group").child("grid-text-input", {
                text: l.qauth.username,
                name: "username"
            }).bind(c).directive("bind", c).up().child("grid-password-input", {
                text: l.qauth.password,
                name: "pass"
            }).bind(u).directive("bind", u).up().up().child("div", {}).set_class("quick-label bee-qconf-group").child("div", {}).set_class("quick-label bee-qconf-line bee-auth-button").child("input-submit", {
                text: l.button.next
            }).bind(d).directive("bind", d).up().child("label", {
                text: l.warning.try_pass_log_again
            }).set_class("quick-label label-auth-common-warning").bind(p).directive("bind", p).up().up().up().up().up().up().child("div", {}).set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-auth-bottom").child("div", {}).set_class("quick-label bee-auth-img").child("bee-auth-bottom-composition", {}).up().up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "auth.js": 2,
        "event-emitter.js": 8,
        "form.js": 10,
        "navi.js": 16,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    85: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            e = e.attr.src || "";
            this.tree = new i("div", {}), this.tree.root().set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-bottom-block").child("img", {
                src: e
            }).set_class("bee-bottom-img").up().up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    86: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("navi.js").navi, i = e.attr, e = i.text || "", s = i.url || "", o = {};
            this.obj = {
                created: function() {
                    o.el.on("click", function() {
                        console.log("cl"), n().go(s);
                    });
                }
            }, this.tree = new a("input", {
                value: e,
                type: "button"
            }), this.tree.root().set_class("bee-button bee-button-line1").bind(o).directive("bind", o);
        };
    }, {
        "navi.js": 16,
        "virtual-dom.js": 26
    } ],
    87: [ function(v, t, e) {
        "use strict";
        var b = v("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = v("../js/bee-quick-lang.js").lang, i = v("event-emitter.js").EventEmiter, s = n(), o = e.attr, n = o.text || "", e = o.name || "";
            var r = o.validator || function(t) {
                return 0 == t.length ? {
                    state: !1,
                    text: s.qerror.field_empty
                } : /^\w*$/.test(t) ? {
                    state: !0,
                    text: ""
                } : {
                    state: !1,
                    text: s.qerror.field_invalid
                };
            }, a = {}, l = {}, c = {}, u = !0, d = !1, p = this.exports = {}, h = new i();
            function _() {
                var t = r(a.el.e.value), e = t.state, t = t.text;
                (u = e) ? l.el.e.style.display = "none" : (l.el.set(t), l.el.e.style.display = "block");
            }
            var f = !1;
            function m(t) {
                c.el.e.src = t ? "/password-vis.svg" : "/password-hid.svg", a.el.e.type = t ? "text" : "password";
            }
            this.obj = {
                created: function() {
                    c.el.on("click", function(t) {
                        m(f = !f);
                    }), p.on = function(t, e) {
                        return h.on(t, e);
                    }, p.is_valid = function() {
                        return u;
                    }, p.is_changed = function() {
                        return d;
                    }, p.get_value = function() {
                        return a.el.e.value;
                    }, p.set_value = function(t) {
                        return a.el.e.value = t;
                    }, p.disabled = function(t) {
                        a.el.disabled(t);
                    }, l.el.e.style.display = "none", p.changed = function() {
                        d = !0, _(), h.emit("change", a.el.e.value);
                    }, p.no_changed = function() {
                        d = !1;
                    }, a.el.on("input", function(t) {
                        d = !0, _(), h.emit("change", a.el.e.value);
                    });
                },
                mounted: function() {
                    m(f);
                }
            }, this.tree = new b("div", {}), this.tree.root().set_class("quick-label bee-password").child("div", {}).set_class("quick-label bee-margin-bottom-10-px").child("div", {}).set_class("quick-label bee-qconf-line").child("label", {
                text: n
            }).set_class("bee-qconf-col1").up().child("span", {}).set_class("input-password").child("input", {
                name: e,
                type: "password"
            }).set_class("bee-qconf-col2").bind(a).directive("bind", a).up().child("img", {
                src: "/password-hid.svg"
            }).bind(c).directive("bind", c).up().up().up().child("span", {}).set_class("bee-warning").child("label", {
                text: s.qerror.field_empty
            }).set_class("bee-qconf-field-error").bind(l).directive("bind", l).up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    88: [ function(t, e, n) {
        "use strict";
        var a = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            var n = {}, i = {}, s = e.attr, e = s.text || "", o = s.default || "", r = this.exports = {};
            this.obj = {
                created: function() {
                    n.el.set(o), r.set_value = function(t) {
                        n.el.set(t);
                    }, r.to_default = function(t) {
                        n.el.set(o);
                    }, r.set_text = function(t) {
                        i.el.set(t);
                    };
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("quick-label bee-line-info").child("div", {}).set_class("quick-label bee-line-info-name").child("label", {
                text: e
            }).set_class("quick-label").bind(i).directive("bind", i).up().up().child("div", {}).set_class("quick-label bee-line-info-value").child("label", {}).bind(n).directive("bind", n).up().up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    89: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            this.tree = new i("div", {}), this.tree.root().set_class("quick-label bee-auth-composition").child("img", {
                alt: "",
                src: "cpe_back4.png"
            }).set_class("cpe-back").up().child("img", {
                src: "cpe_front2.png"
            }).set_class("cpe-front").up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    90: [ function(d, t, e) {
        "use strict";
        var p = d("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = d("../js/bee-quick-lang.js").lang, r = d("system.js").poll, i = n(), s = (e.attr, 
            {});
            function o(t, e) {
                var n = this;
                this.count = 1, t.set(e + this.get_dots()), this.poll = r(500, function() {
                    t.set(e + n.get_dots());
                });
            }
            o.prototype.get_dots = function() {
                return this.count++, 3 < this.count && (this.count = 0), "...".slice(0, this.count);
            }, o.prototype.stop = function() {
                this.poll.cancel();
            };
            var a = void 0, l = {}, c = this.exports = {};
            function u(t, e, n, i) {
                var s = this;
                this.timeCountDown = n, t.set(e + n);
                var o = this.poll = r(1e3, function() {
                    s.timeCountDown--, 0 == s.timeCountDown && (o.cancel(), i()), t.set(e + s.timeCountDown);
                });
            }
            u.prototype.stop = function() {
                this.poll.cancel();
            }, this.obj = {
                created: function() {
                    s.el.show(!1), c.countDown = function(t, e, n) {
                        s.el.show(!0), new u(l.el, t, e, function(t) {
                            s.el.show(!1), n();
                        });
                    }, c.run = function() {
                        s.el.show(!0), a = new o(l.el, i.pending.applying);
                    }, c.stop = function() {
                        s.el.show(!1), a && a.stop();
                    };
                }
            }, this.tree = new p("div", {}), this.tree.root().set_class("quick-label bee-qmenu-pending").bind(s).directive("bind", s).child("label", {
                text: i.pending.applying
            }).set_class("quick-label bee-qmenu-pending-text").bind(l).directive("bind", l).up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    91: [ function(o, t, e) {
        "use strict";
        var r = o("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            (0, o("../js/bee-quick-lang.js").lang)();
            var e = e.attr.text || "", n = {}, i = {}, s = this.exports = {};
            this.obj = {
                created: function() {
                    i.el.show(!1), s.disabled = function() {
                        n.el.disabled(!0);
                    }, s.enabled = function() {
                        n.el.disabled(!1);
                    }, s.on = function(t, e) {
                        n.el.on(t, e);
                    }, s.pending = function(t) {
                        i.el.show(t);
                    };
                }
            }, this.tree = new r("div", {}), this.tree.root().set_class("quick-label input-submit").child("input", {
                value: e,
                type: "button"
            }).set_class("bee-button bee-button-line1").bind(n).directive("bind", n).up().child("img", {
                src: "/pending.svg"
            }).bind(i).directive("bind", i).up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "virtual-dom.js": 26
    } ],
    92: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            this.tree = new i("defs", {}), this.tree.root().child("linearGradient", {
                "xlink:href": "#linearGradient902",
                id: "linearGradient1029",
                x1: "53.117",
                x2: "53.117",
                y1: "138.36",
                y2: "139.97",
                gradientTransform: "translate(1.3702 -.059059)",
                gradientUnits: "userSpaceOnUse"
            }).child("stop", {
                "stop-color": "#0d8327",
                offset: "0"
            }).up().child("stop", {
                "stop-color": "#0d8327",
                "stop-opacity": "0",
                offset: "1"
            }).up().up().child("linearGradient", {
                id: "linearGradient1031",
                x1: "53.117",
                x2: "53.117",
                y1: "138.36",
                y2: "139.97",
                gradientTransform: "matrix(1 0 0 1.3326 2.9766 -46.539)",
                gradientUnits: "userSpaceOnUse"
            }).child("stop", {
                "stop-color": "#0d8327",
                offset: "0"
            }).up().child("stop", {
                "stop-color": "#0d8327",
                "stop-opacity": "0",
                offset: "1"
            }).up().up().child("linearGradient", {
                id: "linearGradient1033",
                x1: "53.117",
                x2: "53.117",
                y1: "138.36",
                y2: "139.97",
                gradientTransform: "matrix(1 0 0 1.7105 4.5357 -99.376)",
                gradientUnits: "userSpaceOnUse"
            }).child("stop", {
                "stop-color": "#0d8327",
                offset: "0"
            }).up().child("stop", {
                "stop-color": "#0d8327",
                "stop-opacity": "0",
                offset: "1"
            }).up().up().child("linearGradient", {
                id: "linearGradient1035",
                x1: "53.117",
                x2: "53.117",
                y1: "138.36",
                y2: "139.97",
                gradientTransform: "matrix(1 0 0 2.043 6.1539 -145.9)",
                gradientUnits: "userSpaceOnUse"
            }).child("stop", {
                "stop-color": "#0d8327",
                offset: "0"
            }).up().child("stop", {
                "stop-color": "#0d8327",
                "stop-opacity": "0",
                offset: "1"
            }).up().up().child("linearGradient", {
                id: "linearGradient1037",
                x1: "53.117",
                x2: "53.117",
                y1: "138.36",
                y2: "139.97",
                gradientTransform: "matrix(1 0 0 2.5343 7.7131 -214.64)",
                gradientUnits: "userSpaceOnUse"
            }).child("stop", {
                "stop-color": "#0d8327",
                offset: "0"
            }).up().child("stop", {
                "stop-color": "#0d8327",
                "stop-opacity": "0",
                offset: "1"
            }).up().up().child("linearGradient", {
                id: "bee-arrow",
                x1: "53.117",
                x2: "53.117",
                y1: "138.36",
                y2: "139.97",
                gradientTransform: "matrix(1 0 0 2.5343 7.7131 -214.64)",
                gradientUnits: "userSpaceOnUse"
            }).child("stop", {
                "stop-color": "#f7a600",
                offset: "0"
            }).up().child("stop", {
                "stop-color": "#ffde01",
                "stop-opacity": "0",
                offset: "1"
            }).up().up().child("clipPath", {
                id: "beeClientHostName"
            }).text("\n\t\t      ").child("rect", {
                x: "2",
                y: "10",
                width: "20",
                height: "30"
            }).up().up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    93: [ function(c, t, e) {
        "use strict";
        var u = c("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = c("../js/bee-quick-lang.js").lang, i = c("event-emitter.js").EventEmiter, n = n(), s = {}, e = e.attr, o = e.text_on || n.button.on, r = e.text_off || n.button.off, a = new i(), l = this.exports = {};
            this.obj = {
                created: function() {
                    var e = !0, n = !1;
                    s.el.set(n ? r : o), l.on = function(t, e) {
                        return a.on(t, e);
                    }, l.get_value = function() {
                        return n;
                    }, l.is_valid = function() {
                        return !0;
                    }, l.is_changed = function() {
                        return e;
                    }, l.no_changed = function() {
                        e = !1;
                    }, l.set_value = function(t) {
                        n = t, s.el.set(n ? r : o), a.emit("set_value", {
                            state: n
                        });
                    }, s.el.on("click", function(t) {
                        n = !n, e = !0, s.el.set(n ? r : o), a.emit("change", {
                            state: n
                        });
                    });
                }
            }, this.tree = new u("button", {}), this.tree.root().set_class("bee-switcher").bind(s).directive("bind", s);
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    94: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("../js/bee-quick-lang.js").lang, i = r("event-emitter.js").EventEmiter, n = (n(), 
            e.attr), e = (n = e.attr).up || "", n = n.down || "", s = new i(), o = this.exports = {};
            this.obj = {
                created: function() {
                    o.on = function(t, e) {
                        return s.on(t, e);
                    };
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("quick-label bee-title").child("label", {
                text: e
            }).set_class("bee-title-up").up().child("label", {
                text: n
            }).set_class("bee-title-down").up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    95: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("../js/bee-quick-lang.js").lang, i = r("event-emitter.js").EventEmiter, n = n(), s = (e.attr, 
            new i()), o = this.exports = {};
            this.obj = {
                created: function() {
                    o.on = function(t, e) {
                        return s.on(t, e);
                    };
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("quick-label bee-twz").child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: n.twz.ip_ok_up,
                down: n.twz.ip_ok_down
            }).up().child("div", {}).set_class("quick-label bee-qconf-content").child("p", {}).set_class("bee-quick-main-text").child("label", {
                text: n.twz.ip_fail
            }).set_class("quick-label").up().child("label", {
                text: n.twz.ip_fail2
            }).set_class("quick-label").up().up().child("div", {}).set_class("bee-qconf-line").child("bee-save-button", {
                text: n.button.main_menu
            }).up().child("bee-save-button", {
                text: n.button.reboot
            }).up().up().up().up().child("div", {}).set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-auth-bottom").child("div", {}).set_class("quick-label bee-auth-img").child("bee-welcome-bottom-composition", {}).up().up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    96: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("../js/bee-quick-lang.js").lang, i = r("event-emitter.js").EventEmiter, n = n(), s = (e.attr, 
            new i()), o = this.exports = {};
            this.obj = {
                created: function() {
                    o.on = function(t, e) {
                        return s.on(t, e);
                    };
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("quick-label bee-twz").child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: n.twz.ip_ok_up,
                down: n.twz.ip_ok_down
            }).up().child("div", {}).set_class("quick-label bee-qconf-content").child("p", {}).set_class("bee-quick-main-text").child("label", {
                text: n.twz.ip_ok
            }).set_class("quick-label").up().up().child("div", {}).set_class("bee-qconf-line").child("bee-save-button", {
                text: n.button.main_menu
            }).up().child("bee-save-button", {
                text: n.button.conf
            }).up().up().up().up().child("div", {}).set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-auth-bottom").child("div", {}).set_class("quick-label bee-auth-img").child("bee-welcome-bottom-composition", {}).up().up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    97: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("../js/bee-quick-lang.js").lang, i = r("event-emitter.js").EventEmiter, n = n(), s = (e.attr, 
            new i()), o = this.exports = {};
            this.obj = {
                created: function() {
                    o.on = function(t, e) {
                        return s.on(t, e);
                    };
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("quick-label bee-twz bee-twz-no-conf").child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: n.twz.no_conf_up,
                down: n.twz.no_conf_down
            }).up().child("div", {}).set_class("quick-label bee-qconf-content").child("p", {}).set_class("bee-quick-main-text").child("label", {
                text: n.twz.no_conf
            }).set_class("quick-label").up().child("label", {
                text: n.twz.no_conf2
            }).set_class("quick-label").up().up().child("div", {}).set_class("bee-qconf-line").child("bee-save-button", {
                text: n.button.next
            }).up().child("bee-save-button", {
                text: n.button.conf
            }).up().up().up().up().child("div", {}).set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-auth-bottom").child("div", {}).set_class("quick-label bee-auth-img").child("bee-welcome-bottom-composition", {}).up().up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    98: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("../js/bee-quick-lang.js").lang, i = r("event-emitter.js").EventEmiter, n = n(), s = (e.attr, 
            new i()), o = this.exports = {};
            this.obj = {
                created: function() {
                    o.on = function(t, e) {
                        return s.on(t, e);
                    };
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("quick-label bee-twz bee-twz-no-conf").child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: n.twz.no_ip_up,
                down: n.twz.no_ip_down
            }).up().child("div", {}).set_class("quick-label bee-qconf-content").child("p", {}).set_class("bee-quick-main-text").child("label", {
                text: n.twz.no_ip
            }).set_class("quick-label").up().child("label", {
                text: n.twz.no_ip2
            }).set_class("quick-label").up().child("label", {
                text: n.twz.no_ip3
            }).set_class("quick-label").up().child("label", {
                text: n.twz.no_ip4
            }).set_class("quick-label").up().up().child("div", {}).set_class("bee-qconf-line").child("bee-save-button", {
                text: n.button.main_menu
            }).up().child("bee-save-button", {
                text: n.button.get_ip
            }).up().up().up().up().child("div", {}).set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-auth-bottom").child("div", {}).set_class("quick-label bee-auth-img").child("bee-welcome-bottom-composition", {}).up().up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    99: [ function(l, t, e) {
        "use strict";
        var c = l("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = l("../js/bee-quick-lang.js").lang, i = l("event-emitter.js").EventEmiter, n = n(), s = l("twz.js").twz, o = (e.attr, 
            {}), r = new i(), a = this.exports = {};
            this.obj = {
                created: function() {
                    a.on = function(t, e) {
                        return r.on(t, e);
                    }, o.exports.on("click", function(t) {
                        o.exports.disabled(!0), o.exports.pending(!0), s().stop();
                    });
                }
            }, this.tree = new c("div", {}), this.tree.root().set_class("quick-label bee-twz bee-twz-no-wan").child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: n.twz.no_wan_up,
                down: n.twz.no_wan_down
            }).up().child("div", {}).set_class("quick-label bee-qconf-content").child("p", {}).set_class("bee-quick-main-text").child("label", {
                text: n.twz.no_wan
            }).set_class("quick-label").up().child("label", {
                text: n.twz.no_wan2
            }).set_class("quick-label").up().up().child("div", {}).set_class("bee-qconf-line").child("bee-save-button", {
                text: n.button.main_menu
            }).bind(o).directive("bind", o).up().up().up().up().child("div", {}).set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-auth-bottom").child("div", {}).set_class("quick-label bee-auth-img").child("bee-twz-nowan-bottom-composition", {}).up().up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "twz.js": 24,
        "virtual-dom.js": 26
    } ],
    100: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            this.tree = new i("div", {}), this.tree.root().set_class("quick-label twz-no-wan-bottom").child("img", {
                src: "cpe_back1.png"
            }).set_class("cpe_back").up().child("div", {}).set_class("quick-label warning").child("bee-twz-nowan-bottom-warning", {}).up().up().child("div", {}).set_class("quick-label arrow").child("arrow", {}).up().up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    101: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("../js/bee-quick-lang.js").lang, i = r("event-emitter.js").EventEmiter, n = n(), s = (e.attr, 
            new i()), o = this.exports = {};
            this.obj = {
                created: function() {
                    o.on = function(t, e) {
                        return s.on(t, e);
                    };
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("quick-label bee-auth-buttom-warning").child("img", {
                src: "help_ico2.png"
            }).up().child("span", {}).set_class("bee-auth-bottom-warning-text").child("label", {
                text: n.warning.no_wan
            }).set_class("quick-label").up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    102: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            this.tree = new i("div", {}), this.tree.root().set_class("quick-label welcome-bottom").child("img", {
                src: "cpe_front2.png"
            }).set_class("cpe_front").up().child("img", {
                src: "cpe_back2.png"
            }).set_class("cpe_back").up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    103: [ function(i, t, e) {
        "use strict";
        var s = i("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = (0, i("../js/bee-quick-lang.js").lang)();
            this.tree = new s("div", {}), this.tree.root().set_class("quick-label bee-welcome").child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: n.welcome.text_up,
                down: n.welcome.text_down
            }).up().child("div", {}).set_class("quick-label bee-qconf-content").child("p", {}).set_class("bee-quick-main-text").child("label", {
                text: n.welcome.text1
            }).set_class("quick-label").up().child("label", {
                text: n.welcome.text2
            }).set_class("quick-label").up().child("label", {
                text: n.welcome.text3
            }).directive("if", !1).up().child("label", {
                text: n.welcome.text4
            }).set_class("quick-label").up().up().child("div", {}).set_class("bee-qconf-line").child("div", {}).set_class("quick-label bee-qconf-col1").child("bee-go-button", {
                text: n.button.next,
                url: "bee-auth"
            }).up().up().up().up().up().child("div", {}).set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-auth-bottom").child("div", {}).set_class("quick-label bee-auth-img").child("bee-welcome-bottom-composition", {}).up().up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "virtual-dom.js": 26
    } ],
    104: [ function(o, t, e) {
        "use strict";
        var r = o("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            (0, o("../js/bee-quick-lang.js").lang)();
            var e = e.attr.text || "", n = {}, i = {}, s = this.exports = {};
            this.obj = {
                created: function() {
                    s.disabled = function() {
                        n.el.disabled(!0);
                    }, s.enabled = function() {
                        n.el.disabled(!1);
                    }, s.pending = function(t) {
                        i.el.show(t);
                    }, s.on = function(t, e) {
                        n.el.on(t, e);
                    };
                }
            }, this.tree = new r("span", {}), this.tree.root().set_class("input-submit").child("input", {
                value: e,
                name: e,
                type: "submit"
            }).set_class("bee-button bee-button-line1").bind(n).directive("bind", n).up().child("img", {
                src: "/pending.svg"
            }).bind(i).directive("bind", i).up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "virtual-dom.js": 26
    } ],
    105: [ function(o, t, e) {
        "use strict";
        var r = o("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = (0, o("data_utility.js").get_capabilities)(), i = (o("navi.js").navi, n.usb_enabled || !1), n = (0, 
            o("../js/bee-quick-lang.js").lang)(), s = {};
            this.obj = {
                created: function() {
                    i || s.el.setClass("bee-quick-main-1-line");
                }
            }, this.tree = new r("div", {}), this.tree.root().set_class("quick-label bee-main-menu").child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: n.qmenu.up,
                down: n.qmenu.down
            }).up().child("p", {}).set_class("bee-quick-margin-left bee-quick-main-text").child("label", {
                text: n.qmenu.text1
            }).set_class("quick-label").up().child("span", {
                text: n.qmenu.bolder1
            }).set_class("text-bold").up().child("label", {
                text: n.qmenu.text2
            }).set_class("quick-label").up().child("span", {
                text: n.qmenu.bolder2
            }).set_class("text-bold").up().child("label", {
                text: n.qmenu.text3
            }).set_class("quick-label").directive("if", i).up().child("span", {
                text: n.qmenu.bolder3
            }).set_class("text-bold").directive("if", i).up().child("label", {
                text: n.qmenu.text4
            }).set_class("quick-label").up().child("span", {
                text: n.qmenu.bolder4
            }).set_class("text-bold").up().child("label", {
                text: n.qmenu.text5
            }).set_class("quick-label").up().child("span", {
                text: n.qmenu.bolder5
            }).set_class("text-bold").up().child("label", {
                text: n.qmenu.text6
            }).set_class("quick-label").up().up().child("div", {}).set_class("bee-quick-main-1-line").child("bee-go-button", {
                text: n.button.quick,
                url: "quick-config"
            }).up().child("bee-go-button", {
                text: n.button.netmap,
                url: "netmap"
            }).up().child("bee-go-button", {
                text: n.button.USB,
                url: "USB"
            }).directive("if", i).up().up().child("div", {}).set_class("bee-quick-main-2-line").bind(s).directive("bind", s).child("bee-go-button", {
                text: n.button.detail_menu,
                url: "status.html"
            }).up().child("bee-go-button", {
                text: n.button.about,
                url: "about"
            }).up().up().up().child("div", {
                text: n.warning.no_configured
            }).set_class("bee-quick-bottom-warning").directive("if", !1).child("img", {
                src: "help_ico2.png"
            }).set_class("bee-ico-info").up().child("span", {}).set_class("bee-quick-bottom-text").child("span", {
                text: n.warning.no_configured
            }).set_class("text-bold").up().child("span", {
                text: n.warning.goto_configured
            }).set_class("text-bold").up().up().up().child("div", {}).set_class("quick-label bee-bottom").child("div", {}).set_class("quick-label bee-bottom-block").child("div", {}).set_class("quick-label bee-bottom-img").child("bee-main-bottom-composition", {}).up().up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "data_utility.js": 4,
        "navi.js": 16,
        "virtual-dom.js": 26
    } ],
    106: [ function(h, t, e) {
        "use strict";
        var _ = h("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = e.attr, i = n.path || {}, e = n.transform || "", n = (0, h("data_utility.js").get_capabilities)(), s = (h("navi.js").navi, 
            n.usb_enabled, (0, h("../js/bee-quick-lang.js").lang)(), {}), o = {}, n = {}, r = {}, a = {}, l = {}, c = !0, u = this.exports = {};
            function d(t) {
                8 < t.length && (t = t.substr(0, 8) + ".."), l.el.set(t);
            }
            var p = {};
            this.obj = {
                created: function() {
                    r.el.e.setAttribute("visibility", "hidden"), s.el.e.setAttribute("visibility", "hidden"), 
                    o.el.e.setAttribute("visibility", "hidden"), u.set_as_wifi = function(t, e, n) {
                        c = !1, r.el.e.setAttribute("visibility", "visibility"), o.el.e.setAttribute("visibility", "visibility"), 
                        s.el.e.setAttribute("visibility", "hidden"), d(e), a.el.set(t), i.show(!1), p.isWifi = !0, 
                        p.wlan_idx = n;
                    }, u.set_detail_window = function(t) {
                        p.window = t;
                    }, u.set_detail_data = function(t) {
                        p.data = t;
                    }, u.set_wlan_stat = function(t) {
                        p.wlan_stat = t;
                    }, u.set_wlan_bss = function(t) {
                        p.wlan_bss = t;
                    }, u.set_wlan_sta_info = function(t) {
                        p.wlan_sta_info = t;
                    }, u.set_as_lan = function(t, e) {
                        c = !1, r.el.e.setAttribute("visibility", "visibility"), o.el.e.setAttribute("visibility", "hidden"), 
                        s.el.e.setAttribute("visibility", "visibility"), d(e), a.el.set(t), i.show(!0), 
                        p.isWifi = !1;
                    }, u.disabled = function() {
                        c = !0, r.el.e.setAttribute("visibility", "hidden"), s.el.e.setAttribute("visibility", "visibility"), 
                        i.show(!1);
                    }, r.el.on("click", function() {
                        p.window.exports.active(p);
                    }), u.is_free = function() {
                        return c;
                    };
                }
            }, this.tree = new _("g", {
                transform: e
            }), this.tree.root().set_class("netmap-client").bind(r).directive("bind", r).child("rect", {
                "stroke-opacity": ".95259",
                "stroke-width": ".65",
                x: "0",
                y: "0",
                width: "27.343",
                height: "26.541",
                ry: "2.205",
                opacity: ".462",
                fill: "#fff",
                stroke: "#b3b3b3"
            }).bind(s).directive("bind", s).up().child("g", {
                transform: "translate(2 25)"
            }).bind(o).directive("bind", o).child("rect", {
                x: "0.929",
                y: "3.28",
                width: "1.0688",
                height: "1.5628",
                ry: ".24042",
                fill: "url(#linearGradient1029)"
            }).up().child("rect", {
                x: "2.535",
                y: "2.81",
                width: "1.0688",
                height: "2.0825",
                ry: ".32038",
                fill: "url(#linearGradient1031)"
            }).up().child("rect", {
                x: "4.094",
                y: "2.26",
                width: "1.0688",
                height: "2.6731",
                ry: ".25769",
                fill: "url(#linearGradient1033)"
            }).up().child("rect", {
                x: "5.712",
                y: "1.74",
                width: "1.0688",
                height: "3.1928",
                ry: ".25496",
                fill: "url(#linearGradient1035)"
            }).up().child("rect", {
                x: "7.271",
                y: "0.97",
                width: "1.0688",
                height: "3.9605",
                ry: ".23134",
                fill: "url(#linearGradient1037)"
            }).up().up().child("g", {
                transform: "translate(2 2)"
            }).child("g", {
                transform: "scale(.3)"
            }).child("image", {
                href: "laptop_pc.png",
                width: "60"
            }).bind(n).directive("bind", n).up().up().up().child("text", {
                "clip-path": "url(#beeClientHostName)",
                x: "2",
                y: "20"
            }).set_class("client-name").bind(l).directive("bind", l).text("rb-Blafs...").up().child("text", {
                x: "2",
                y: "25"
            }).set_class("client-ip").bind(a).directive("bind", a).text("192.168.100.100").up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "data_utility.js": 4,
        "navi.js": 16,
        "virtual-dom.js": 26
    } ],
    107: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            this.tree = new i("div", {
                transform: transform
            }), this.tree.root().set_class("quick-label").bind(root).directive("bind", root).child("rect", {
                "stroke-opacity": ".95259",
                "stroke-width": ".65",
                x: "0",
                y: "0",
                width: "27.343",
                height: "26.541",
                ry: "2.205",
                opacity: ".462",
                fill: "none",
                stroke: "#b3b3b3"
            }).bind(border).directive("bind", border).up().child("g", {
                transform: "translate(2 25)"
            }).bind(wifi).directive("bind", wifi).child("rect", {
                x: "0.929",
                y: "3.28",
                width: "1.0688",
                height: "1.5628",
                ry: ".24042",
                fill: "url(#linearGradient1029)"
            }).up().child("rect", {
                x: "2.535",
                y: "2.81",
                width: "1.0688",
                height: "2.0825",
                ry: ".32038",
                fill: "url(#linearGradient1031)"
            }).up().child("rect", {
                x: "4.094",
                y: "2.26",
                width: "1.0688",
                height: "2.6731",
                ry: ".25769",
                fill: "url(#linearGradient1033)"
            }).up().child("rect", {
                x: "5.712",
                y: "1.74",
                width: "1.0688",
                height: "3.1928",
                ry: ".25496",
                fill: "url(#linearGradient1035)"
            }).up().child("rect", {
                x: "7.271",
                y: "0.97",
                width: "1.0688",
                height: "3.9605",
                ry: ".23134",
                fill: "url(#linearGradient1037)"
            }).up().up().child("g", {
                transform: "translate(2 2)"
            }).child("g", {
                transform: "scale(.3)"
            }).child("image", {
                href: "laptop_pc.png",
                width: "60"
            }).bind(img).directive("bind", img).up().up().up().child("text", {
                x: "2",
                y: "20",
                width: "5",
                overflow: "hidden"
            }).set_class("client-name").text("rb-Blafs...").up().child("text", {
                x: "2",
                y: "25",
                overflow: "hidden"
            }).set_class("client-ip").text("192.168.100.100").up().child("script", {
                type: "text/javascript"
            }).text('\n\tvar attr = node.attr;\n\tvar path = attr.path || {};\n\tvar transform = attr.transform||"";\n\tconst  {get_capabilities} = require(\'data_utility.js\');\n\tvar C = get_capabilities();\n\tconst  {navi} = require(\'navi.js\');\n\tvar usb_enabled =C.usb_enabled || false;\n\n\tconst {lang} = require(\'../js/bee-quick-lang.js\');\n\tvar l = lang();\n\tvar line2 = {};\n\n\tvar quick_config = {};\n\tvar no_configured = false;\n\tvar imggg ={}\n\tvar ic ={}\n\tvar test = {}\n\tvar border = {};\n\tvar wifi = {};\n\tvar img = {};\n\tvar root = {};\n\n\n\tvar exports = this.exports = {};\n\tthis.obj = {\n\t\tcreated:function(){\n\n\t\t\t// root.el.e.setAttribute("visibility", "hidden");\n\t\t\t// border.el.e.setAttribute("visibility", "hidden");\n\t\t\t// wifi.el.e.setAttribute("visibility", "hidden");\n\t\t\t// exports.set_as_wifi = function(){\n\t\t\t// \troot.el.e.setAttribute("visibility", "visibility");\n\t\t\t// \twifi.el.e.setAttribute("visibility", "visibility");\n\t\t\t// \tborder.el.e.setAttribute("visibility", "hidden");\n\t\t\t// \tpath.show(false);\n\t\t\t// }\n\t\t\t// exports.set_as_lan = function(){\n\t\t\t// \troot.el.e.setAttribute("visibility", "visibility");\n\t\t\t// \twifi.el.e.setAttribute("visibility", "hidden");\n\t\t\t// \tborder.el.e.setAttribute("visibility", "visibility");\n\t\t\t// \tpath.show(true);\n\t\t\t// }\n\t\t\t// exports.disabled = function(){\n\t\t\t// \troot.el.e.setAttribute("visibility", "hidden");\n\t\t\t// \tborder.el.e.setAttribute("visibility", "visibility");\n\t\t\t// \tpath.show(true);\n\t\t\t// }\n\n\n\t\t}\n\t}\n\n').up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    108: [ function(h, t, e) {
        "use strict";
        var _ = h("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            e.attr, e.attr.path;
            var e = h("../js/bee-quick-lang.js").lang, n = (h("dom-maker.js").pretty_byte_traffic, 
            e()), i = {}, s = {}, o = {}, r = {}, a = {}, l = {}, c = {}, u = {}, d = {};
            var p = this.exports = {};
            this.obj = {
                created: function() {
                    i.el.show(!1), c.el.on("click", function(t) {
                        return t.preventDefault(), i.el.show(!1), !1;
                    }), p.active = function(t) {
                        var e = t.data;
                        console.log(t), s.exports.set_value(e.hostName || "..."), o.exports.set_value(e.mac || "..."), 
                        r.exports.set_value(e.ip || "..."), a.exports.set_value((e = t).isWifi ? n.netmap.wifi + " " + (e = e, 
                        1 == {
                            end: 0,
                            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                            BUILD: "debug"
                        }.CONFIG_LUNA ? 1 == e.wlan_idx ? "2G" : "5G" : 1 == e.wlan_idx ? "5G" : "2G") : n.netmap.lan), 
                        l.exports.set_value((e = t).isWifi ? e.data.txRate + " ???±??N?/c" : n.netmap.speed_100), 
                        u.el.show(t.isWifi), d.el.show(t.isWifi), t.isWifi && (u.exports.set_value(t.wlan_bss[t.wlan_idx].ssid), 
                        d.exports.set_value(function(t) {
                            if (t.isWifi) {
                                t = t.wlan_sta_info[t.data.mac.toUpperCase()];
                                return t ? parseInt(t.rssi) - 100 + " dBm" : "...";
                            }
                            return n.netmap.lan;
                        }(t))), i.el.show(!0);
                    };
                }
            }, this.tree = new _("div", {}), this.tree.root().set_class("netmap-popup-detail").bind(i).directive("bind", i).child("div", {}).set_class("quick-label popup-header").child("label", {
                text: n.netmap.help_title,
                for: ""
            }).set_class("quick-label").up().child("a", {
                href: ""
            }).set_class("link-ico link-ico-close").bind(c).directive("bind", c).up().up().child("div", {}).set_class("quick-label netmap-info").child("bee-line-info", {
                text: n.netmap.name_dev,
                default: "..."
            }).bind(s).directive("bind", s).up().child("bee-line-info", {
                text: n.qabout.mac,
                default: "..."
            }).bind(o).directive("bind", o).up().child("bee-line-info", {
                text: n.netmap.ip,
                default: "..."
            }).bind(r).directive("bind", r).up().child("bee-line-info", {
                text: n.netmap.type_dev,
                default: "..."
            }).bind(a).directive("bind", a).up().child("bee-line-info", {
                text: n.qsettings.ssid,
                default: "..."
            }).bind(u).directive("bind", u).up().child("bee-line-info", {
                text: n.netmap.speed,
                default: "..."
            }).bind(l).directive("bind", l).up().child("bee-line-info", {
                text: n.netmap.signal,
                default: "..."
            }).bind(d).directive("bind", d).up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "dom-maker.js": 5,
        "virtual-dom.js": 26
    } ],
    109: [ function(B, t, e) {
        "use strict";
        var H = B("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = (0, B("data_utility.js").get_capabilities)(), i = (B("navi.js").navi, B("system.js")), s = i.rpc, o = i.poll, r = (n.usb_enabled, 
            (0, B("../js/bee-quick-lang.js").lang)()), a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = {}, _ = {}, f = {}, m = {}, v = {}, b = {}, g = {}, x = {}, w = {}, y = {}, j = {}, k = [ u, d ], N = [ u, p, h ], I = [ u, p, _ ], E = [ u, p, f ], P = [ u, m ], A = [ v, b ], C = [ v, g, x ], D = [ v, g, w ], L = [ v, g, y ], T = [ v, j ], q = {}, S = {}, O = {}, R = {}, F = {}, M = {}, G = {}, U = {}, i = {}, n = {}, V = [ O, R, F, M, G, U, i, n, q, S ];
            function W(n) {
                n.visibility = !1, n.forEach(function(t) {
                    t.visibility = !1, t.el.e.setAttribute("visibility", "hidden"), t.paths || (t.paths = []), 
                    t.paths.push(n), console.log(t);
                }), n.show = function(t) {
                    var e;
                    e = t, (t = n).visibility = e, t.filter(function(t) {
                        if (t.paths.filter(function(t) {
                            return t.visibility;
                        }).length) {
                            if (!t.visibility) return t.visibility = !0;
                        } else if (t.visibility) return !(t.visibility = !1);
                        return !1;
                    }).forEach(function(t) {
                        t.el.e.setAttribute("visibility", t.visibility ? "visibility" : "hidden");
                    });
                };
            }
            var z = void 0;
            this.obj = {
                created: function() {
                    function t() {
                        Promise.all([ s("wlan_clients_list", {}), s("lan_clients_list", {}), s("wlan_stats", {
                            wlan_idx: 0,
                            list: [ "tx_bytes", "rx_bytes" ]
                        }), s("wlan_stats", {
                            wlan_idx: 1,
                            list: [ "tx_bytes", "rx_bytes" ]
                        }), s("wlan_bss_info", {
                            wlan_idx: 0,
                            virtual_idx: 0
                        }), s("wlan_bss_info", {
                            wlan_idx: 1,
                            virtual_idx: 0
                        }), s("wlan_proc_sta_info", {}) ]).then(function(e) {
                            V.forEach(function(t) {
                                return t.exports.disabled();
                            }), V.forEach(function(t) {
                                return t.exports.set_detail_window(c);
                            });
                            var n = 0;
                            e[1].forEach(function(t) {
                                n >= V.length || (V[n].exports.set_as_lan(t.ip || "", t.hostName || ""), V[n].exports.set_detail_data(t), 
                                n++);
                            });
                            var n = V.length - 1, i = function(t) {
                                var e, n = {};
                                for (e in t) t[e].forEach(function(t) {
                                    n[t.hwaddr.toUpperCase().match(/.{1,2}/g).join(":")] = {
                                        rssi: t.rssi,
                                        wlan: e
                                    };
                                });
                                return n;
                            }(e[6]);
                            e[0].forEach(function(t) {
                                n <= 0 && V[n].exports.is_free() || (V[n].exports.set_wlan_stat([ e[2], e[3] ]), 
                                V[n].exports.set_wlan_bss([ e[4], e[5] ]), V[n].exports.set_wlan_sta_info(i), V[n].exports.set_as_wifi(t.ip || "", t.hostName || "", t.wlan_idx), 
                                V[n].exports.set_detail_data(t), n--);
                            });
                        });
                    }
                    l.el.show(!1), W(k), W(N), W(I), W(E), W(P), W(A), W(C), W(D), W(L), W(T), k.show(!1), 
                    N.show(!1), I.show(!1), E.show(!1), P.show(!1), A.show(!1), C.show(!1), D.show(!1), 
                    L.show(!1), T.show(!1), t(), z = o(1e3, t);
                },
                destroyed: function() {
                    z && z.cancel();
                }
            }, this.tree = new H("div", {}), this.tree.root().set_class("quick-label bee-netmap").child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("netmap-popup-detail", {}).bind(c).directive("bind", c).up().child("bee-title", {
                up: r.netmap.up,
                down: r.netmap.down
            }).up().child("div", {}).set_class("quick-label").bind(l).directive("bind", l).child("img", {
                src: "laptop.svg"
            }).up().up().child("div", {}).set_class("quick-label netmap").child("svg", {
                "xmlns:xlink": "http://www.w3.org/1999/xlink",
                width: "152.35mm",
                height: "114.8mm",
                version: "1.1",
                viewBox: "0 0 152.35 114.8",
                xmlns: "http://www.w3.org/2000/svg"
            }).child("bee-svg-gradients", {}).up().child("g", {
                "stroke-width": ".3px",
                fill: "none",
                stroke: "#cfcfcf"
            }).child("path", {
                d: "m88 53 h10"
            }).bind(u).directive("bind", u).up().child("path", {
                d: "m98 53 v-30"
            }).bind(d).directive("bind", d).up().child("path", {
                d: "m98 53 h20"
            }).bind(p).directive("bind", p).up().child("path", {
                d: "m118 53 v-38 h10"
            }).bind(h).directive("bind", h).up().child("path", {
                d: "m118 53 h10"
            }).bind(_).directive("bind", _).up().child("path", {
                d: "m118 53 v38 h10"
            }).bind(f).directive("bind", f).up().child("path", {
                d: "m98 53 v30"
            }).bind(m).directive("bind", m).up().text(" \n\n\t\t\t\t\t\t").child("path", {
                d: "m64 53 h-10"
            }).bind(v).directive("bind", v).up().child("path", {
                d: "m54 53 v-30"
            }).bind(b).directive("bind", b).up().text(" \n\t\t\t\t\t\t").child("path", {
                d: "m54 53 h-20"
            }).bind(g).directive("bind", g).up().text(" \n\t\t\t\t\t\t").child("path", {
                d: "m34 53 v-38 h-10"
            }).bind(x).directive("bind", x).up().child("path", {
                d: "m34 53 h-10"
            }).bind(w).directive("bind", w).up().child("path", {
                d: "m34 53 v38 h-10"
            }).bind(y).directive("bind", y).up().child("path", {
                d: "m54 83 v-30"
            }).bind(j).directive("bind", j).up().up().child("netmap-client", {
                path: C,
                transform: "translate(0 7)"
            }).bind(q).directive("bind", q).up().child("netmap-client", {
                path: A,
                transform: "translate(41 0)"
            }).bind(S).directive("bind", S).up().child("netmap-client", {
                path: k,
                transform: "translate(82 0)"
            }).bind(O).directive("bind", O).up().child("netmap-client", {
                path: N,
                transform: "translate(124 7)"
            }).bind(R).directive("bind", R).up().child("netmap-client", {
                path: I,
                transform: "translate(124 42)"
            }).bind(F).directive("bind", F).up().child("netmap-client", {
                path: E,
                transform: "translate(124 78)"
            }).bind(M).directive("bind", M).up().child("netmap-client", {
                path: P,
                transform: "translate(83 86)"
            }).bind(G).directive("bind", G).up().child("netmap-client", {
                path: T,
                transform: "translate(41 86)"
            }).bind(U).directive("bind", U).up().child("netmap-client", {
                path: L,
                transform: "translate(0 78)"
            }).bind(i).directive("bind", i).up().child("netmap-client", {
                path: D,
                transform: "translate(0 42)"
            }).bind(n).directive("bind", n).up().child("g", {
                transform: "translate(50 35)"
            }).child("g", {
                transform: "scale(.8)"
            }).child("image", {
                href: "3_Menu_page.png",
                width: "60"
            }).up().up().up().child("g", {
                transform: "translate(50 35)"
            }).bind(a).directive("bind", a).up().up().up().child("div", {}).set_class("quick-label bee-quick-main-1-line").child("bee-go-button", {
                text: r.button.back,
                url: "main"
            }).up().up().up().child("div", {
                text: r.warning.no_configured
            }).set_class("quick-label bee-quick-bottom-warning").directive("if", !1).child("img", {
                src: "help_ico2.png"
            }).set_class("bee-ico-info").up().child("span", {}).set_class("quick-label bee-quick-bottom-text").child("span", {
                text: r.warning.no_configured
            }).set_class("quick-label text-bold").up().child("span", {
                text: r.warning.goto_configured
            }).set_class("quick-label text-bold").up().up().up().child("bee-bottom", {
                src: "3_Menu_page.png"
            }).up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "data_utility.js": 4,
        "navi.js": 16,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    110: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("data_utility.js").get_capabilities, i = r("event-emitter.js").EventEmiter, c = r("../../cpe/js/wifi.js").wlan_get_data, u = (r("dom-maker.js").AddressTypesEnum, 
            r("dom-maker.js").WAN_STATUS_T, r("system.js").rpc), d = r("form.js").bind_2_input, p = r("system.js").poll, h = (r("../js/bee-custom.js").wan_status_to_text, 
            r("twz.js").twz, r("../../cpe/js/cpe.js").cpe, r("../../cpe/js/multiwan.js").multiwan_packet), n = n(), _ = (r("navi.js").navi, 
            n.usb_enabled, (0, r("../js/bee-quick-lang.js").lang)()), f = (t.globality, {}), m = {}, v = {}, b = {}, g = {}, x = {}, w = {}, y = {}, j = {}, k = {}, N = {}, I = {}, E = {}, P = {}, A = {}, C = {}, D = {}, L = {}, T = {};
            function s(t) {
                return /^\w*$/.test(t) ? t.length < 8 ? (console.log("less than 8 letters"), S.emit("form-invalid", {}), 
                {
                    state: !1,
                    text: _.qerror.pass_less_8
                }) : (S.emit("form-valid", {}), {
                    state: !0,
                    text: ""
                }) : (S.emit("form-invalid", {}), {
                    state: !1,
                    text: _.qerror.field_invalid
                });
            }
            function q(i, s) {
                i.el.on("click", function(t) {
                    var e = s.el.e.style.display, n = i.el.e.className;
                    v.el.e.style.display = "none", f.el.e.style.display = "none", g.el.e.style.display = "none", 
                    b.el.e.className = "bee-qconf-help-off", m.el.e.className = "bee-qconf-help-off", 
                    x.el.e.className = "bee-qconf-help-off", s.el.e.style.display = "none" == e ? "inline-block" : "none", 
                    i.el.e.className = "bee-qconf-help" == n ? "bee-qconf-help-off" : "bee-qconf-help";
                });
            }
            var S = new i(), O = [ w, P, y, j, k, N, I, E, C, T ], o = [ w, P, y, j, k, N, I, E, T ];
            function R() {
                var t = O.filter(function(t) {
                    return !t.exports.is_valid();
                }), e = o.filter(function(t) {
                    return /[^a-zA-Z0-9_\.\\/\!#$&*-]/.test(t.exports.get_value());
                });
                j.exports.get_value().length;
                t.length || 0 !== e.length ? (S.emit("form-invalid", {}), D.exports.disabled()) : (S.emit("form-valid", {}), 
                D.exports.enabled());
            }
            this.obj = {
                created: function() {
                    d(w, P), d(y, j), d(k, N), d(I, E), T.exports.on("set_value", function(t) {
                        E.exports.disabled(!t.state), I.exports.disabled(!t.state);
                    }), T.exports.on("change", function(t) {
                        E.exports.disabled(!t.state), I.exports.disabled(!t.state);
                    }), D.exports.disabled(), S.on("form-valid", function(t) {
                        return D.exports.enabled();
                    }), S.on("form-invalid", function(t) {
                        return D.exports.disabled();
                    }), v.el.e.style.display = "none", g.el.e.style.display = "none", f.el.e.style.display = "none", 
                    b.el.e.className = "bee-qconf-help-off", x.el.e.className = "bee-qconf-help-off", 
                    m.el.e.className = "bee-qconf-help-off", q(b, v), q(x, g), q(m, f), O.forEach(function(t) {
                        return t.exports.on("change", R);
                    });
                    var e = [ "index", "allocated", "AddressType", "vlan", "vlanid", "vlanpriority", "dnsAuto", "wanIfDns1", "wanIfDns2", "wanIfDns3", "drv_ip", "drv_mask", "ipv6Enable", "wanMacAddr", "ipv6Addr", "gateway", "ipv6Prefix", "drv_status", "pppPassword", "pppUserName", "wanType", "iface", "appType", "cwmpInform", "cwmpStatus", "dnsTotal", "drv_gateway", "ifindex", "ipAddr", "ipVersion", "l2tp_resolved_vpn", "login", "name", "netMask", "parentWanIdx", "password", "portMap", "pppServer", "vlan", "wanIfDnsList", "wanStatus", "isDefault" ];
                    function n() {
                        var t = w.exports.get_value(), e = P.exports.get_value();
                        return u("multiwan_alloc_l2tp", {
                            login: t,
                            password: e,
                            server: "tp.internet.beeline.ru",
                            isDefault: !0,
                            tag: {
                                vid: 0,
                                vprio: 0
                            }
                        }).then(u("multiwan_acl_status_get", {}).then(function(t) {
                            t.allocated || u("multiwan_add_acl", {
                                ip: "0.0.0.0",
                                mask: "0.0.0.0",
                                iface_type: 2,
                                service: 4,
                                enabled: 0
                            });
                        }));
                    }
                    function i() {
                        if (y.exports.is_changed()) {
                            var t = {
                                ssid: y.exports.get_value(),
                                pass: j.exports.get_value()
                            };
                            return u("wlan_set", {
                                wlan_idx: 0,
                                wifi: t
                            });
                        }
                        return Promise.resolve(!0);
                    }
                    function s() {
                        if (k.exports.is_changed()) {
                            var t = {
                                ssid: k.exports.get_value(),
                                pass: N.exports.get_value()
                            };
                            return u("wlan_set", {
                                wlan_idx: 1,
                                wifi: t
                            });
                        }
                        return Promise.resolve(!0);
                    }
                    function o() {
                        if (C.exports.is_changed()) {
                            var t, e = C.exports.get_value();
                            return 1 == e[0] && 0 == e[1] ? t = 1 : 1 == e[1] && 0 == e[0] ? t = 2 : 1 == e[0] && 1 == e[1] && (t = 3), 
                            u("multiwan_alloc_bridge", {
                                vid: 0,
                                vprio: 0,
                                lan_idx: t
                            });
                        }
                        return Promise.resolve(!0);
                    }
                    function r() {
                        if (T.exports.is_changed()) {
                            var t = {
                                disabled: !T.exports.get_value()
                            };
                            return u("virtual_wlan_set", {
                                virtual_idx: 2,
                                wlan_idx: 0,
                                wifi: t
                            });
                        }
                        return Promise.resolve(!0);
                    }
                    var a = 4;
                    function l() {
                        if (I.exports.is_changed()) {
                            var t = E.exports.get_value(), e = {
                                ssid: I.exports.get_value(),
                                pass: t
                            };
                            return 0 == t.length ? e.wpa_auth = 0 : (e.wlan_encrypt = a, e.wpaCipher = 1, e.wpa2Cipher = 2, 
                            e.wpa_auth = 2), u("virtual_wlan_set", {
                                wlan_idx: 0,
                                virtual_idx: 2,
                                wifi: e
                            });
                        }
                        return Promise.resolve(!0);
                    }
                    function t() {
                        return Promise.all([ u("rpc_apmib_get", {
                            list: [ "fw_version", "default_wan_index", "hw_version" ]
                        }), h().get_data(e) ]).then(function(t) {
                            0 == t[1].filter(function(t) {
                                return t.allocated && t.isDefault;
                            })[0].wanStatus ? (L.exports.set_value(_.cpe_status.not_connected), L.el.setClass("fail")) : L.exports.set_value(_.cpe_status.connect);
                        });
                    }
                    t();
                    p(1e3, t);
                    Promise.all([ h().get_data(e), c([ "wlanFuncOFF", "ssid", "pass", "channel", "channelBonding", "standart", "encryption", "RFPower", "tx_beamforming", "disable", "hiddenSSID" ]), u("virtual_wlan_get", {
                        wlan_idx: 0,
                        virtual_idx: 2
                    }) ]).then(function(t) {
                        var e = t[0].filter(function(t) {
                            return t.allocated && t.isDefault && "L2TP" == t.wanType;
                        });
                        return 0 == e.length ? (w.exports.set_value(""), P.exports.set_value("")) : (w.exports.set_value(e[0].pppUserName), 
                        P.exports.set_value(e[0].pppPassword)), y.exports.set_value(t[1][0].ssid), j.exports.set_value(t[1][0].pass), 
                        k.exports.set_value(t[1][1].ssid), N.exports.set_value(t[1][1].pass), T.exports.set_value(!t[2].disabled), 
                        I.exports.set_value(t[2].ssid), E.exports.set_value(t[2].pass), 0 != t[0].filter(function(t) {
                            return t.allocated && "BRIDGE" == t.wanType;
                        }).length ? u("ports_map_get", {}).then(function(t) {
                            return 1 == t && C.exports.set_value([ !0, !(t = [ !0 ]) ]), 2 == t ? C.exports.set_value([ !(t = [ !0 ]), !0 ]) : 3 == t && (t = [ !0 ], 
                            C.exports.set_value([ !0, !0 ])), !0;
                        }) : (C.exports.set_value([ !1, !1 ]), !0);
                    }), D.exports.on("click", function(t) {
                        A.exports.run(), (w.exports.is_changed() ? h().get_data(e).then(function(t) {
                            var e = t.filter(function(t) {
                                return t.allocated && t.isDefault && "L2TP" == t.wanType;
                            });
                            return 0 != e.length ? u("multiwan_set", {
                                iface: e[0].iface,
                                list: {
                                    pppUserName: login,
                                    pppPassword: password
                                }
                            }) : 0 == t.filter(function(t) {
                                return t.allocated && "IPOE" == t.wanType;
                            }).length ? u("multiwan_alloc_ipoe", {
                                vid: 0,
                                vprio: 0
                            }).then(n) : void n();
                        }) : Promise.resolve(!0)).then(i).then(s).then(o).then(r).then(l).then(function() {
                            return u("apply", {});
                        }).then(function() {
                            A.exports.stop(), D.exports.disabled(), O.forEach(function(t) {
                                return t.exports.no_changed();
                            });
                        });
                    });
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("quick-label bee-qconf").child("bee-qmenu-pending", {}).bind(A).directive("bind", A).up().child("div", {}).set_class("quick-label bee-logo").child("img", {
                src: "logo.png"
            }).up().up().child("div", {}).set_class("quick-label bee-quick-content").child("bee-title", {
                up: _.qsettings.up,
                down: _.qsettings.down
            }).up().child("div", {}).set_class("quick-label bee-qconf-content bee-qconf-settings").child("div", {}).set_class("quick-label bee-qconf-group margin-bottom-24").child("div", {}).set_class("quick-label bee-qconf-colon1").child("label", {
                text: _.qsettings.home_i
            }).set_class("quick-label bee-qconfig-label").up().child("grid-text-input", {
                text: _.qsettings.username,
                name: "l2tp_login"
            }).set_class("quick-label").bind(w).directive("bind", w).up().child("grid-password-input", {
                text: _.qsettings.password,
                name: "l2tp_pass"
            }).set_class("quick-label").bind(P).directive("bind", P).up().child("div", {}).set_class("quick-label bee-qconf-line").child("bee-line-info", {
                text: _.qabout.state,
                default: "..."
            }).set_class("quick-label").bind(L).directive("bind", L).up().up().up().child("div", {}).set_class("quick-label bee-qconf-help-1").child("div", {}).set_class("quick-label bee-qconf-help ").bind(b).directive("bind", b).child("img", {
                src: "Help_ico.png"
            }).up().up().child("div", {}).set_class("quick-label bee-qconfig-help-block").bind(v).directive("bind", v).child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.l2tp_help
            }).set_class("quick-label").up().up().child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.username
            }).set_class("quick-label text-bold").up().child("span", {}).text(": ").up().child("span", {
                text: _.qsettings.username_help
            }).set_class("quick-label").up().up().child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.password
            }).set_class("quick-label text-bold").up().child("span", {}).text(": ").up().child("span", {
                text: _.qsettings.password_help
            }).set_class("quick-label").up().up().child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.status
            }).set_class("quick-label text-bold").up().child("span", {}).text(": ").up().child("span", {
                text: _.qsettings.status_help
            }).set_class("quick-label").up().up().up().up().up().child("div", {}).set_class("quick-label bee-qconf-group").child("div", {}).set_class("quick-label bee-qconf-colon1").child("label", {
                text: _.qsettings.wifi
            }).set_class("quick-label bee-qconfig-label").up().child("grid-text-input", {
                text: _.qsettings.ssid_2,
                name: "wifi2_login"
            }).set_class("quick-label").bind(y).directive("bind", y).up().child("grid-password-input", {
                validator: s,
                text: _.qsettings.password,
                name: "wifi2_pass"
            }).set_class("quick-label").bind(j).directive("bind", j).up().child("grid-text-input", {
                text: _.qsettings.ssid_5,
                name: "wifi5_login"
            }).set_class("quick-label").bind(k).directive("bind", k).up().child("grid-password-input", {
                validator: s,
                text: _.qsettings.password,
                name: "wifi5_pass"
            }).set_class("quick-label").bind(N).directive("bind", N).up().up().child("div", {}).set_class("quick-label bee-qconf-help-2").child("div", {}).set_class("quick-label bee-qconf-help ").bind(x).directive("bind", x).child("img", {
                src: "Help_ico.png"
            }).up().up().child("div", {}).set_class("quick-label bee-qconfig-help-block").bind(g).directive("bind", g).child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.wifi_help
            }).set_class("quick-label").up().up().child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.ssid
            }).set_class("quick-label text-bold").up().child("span", {}).text(": ").up().child("span", {
                text: _.qsettings.ssid_help
            }).set_class("quick-label").up().up().child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.password
            }).set_class("quick-label text-bold").up().child("span", {}).text(": ").up().child("span", {
                text: _.qsettings.wifi_password_help
            }).set_class("quick-label").up().up().up().up().up().child("div", {}).set_class("quick-label bee-qconf-group").child("div", {}).set_class("quick-label bee-qconf-colon1").child("span", {}).set_class("quick-label bee-qconfig-label").child("label", {
                text: _.qsettings.virtual_wifi
            }).set_class("quick-label").up().child("bee-switcher", {}).bind(T).directive("bind", T).up().up().child("grid-text-input", {
                text: _.qsettings.ssid,
                name: "virtual_wifi_ssid"
            }).set_class("quick-label").bind(I).directive("bind", I).up().child("grid-password-input", {
                validator: function(t) {
                    return /^\w*$/.test(t) ? t.length < 8 ? (S.emit("form-invalid", {}), {
                        state: !1,
                        text: _.qerror.pass_less_8
                    }) : (S.emit("form-valid", {}), {
                        state: !0,
                        text: ""
                    }) : (S.emit("form-invalid", {}), {
                        state: !1,
                        text: _.qerror.field_invalid
                    });
                },
                text: _.qsettings.password,
                name: "virtual_wifi_pass"
            }).set_class("quick-label").bind(E).directive("bind", E).up().up().child("div", {}).set_class("quick-label bee-qconf-help-3").child("div", {}).set_class("quick-label bee-qconf-help bee-qconf-help-ico-3").bind(m).directive("bind", m).child("img", {
                src: "Help_ico.png"
            }).up().up().child("div", {}).set_class("quick-label bee-qconfig-help-block bee-qconf-help-3").bind(f).directive("bind", f).child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.vwifi_help
            }).set_class("quick-label").up().up().child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.ssid
            }).set_class("quick-label text-bold").up().child("span", {}).text(": ").up().child("span", {
                text: _.qsettings.vssid_help
            }).set_class("quick-label").up().up().child("div", {}).set_class("quick-label bee-qconfig-help-group").child("span", {
                text: _.qsettings.password
            }).set_class("quick-label text-bold").up().child("span", {}).text(": ").up().child("span", {
                text: _.qsettings.vpass_help
            }).set_class("quick-label").up().up().up().up().up().child("wan-bridge", {}).bind(C).directive("bind", C).up().child("div", {}).set_class("quick-label bee-quick-main-1-line qconf-submit-buttons").child("bee-go-button", {
                text: _.button.back,
                url: "main"
            }).up().child("bee-save-button", {
                text: _.button.save,
                url: "main"
            }).bind(D).directive("bind", D).up().up().up().up().child("bee-bottom", {
                src: "3_Menu_page.png"
            }).up();
        };
    }, {
        "../../cpe/js/cpe.js": 67,
        "../../cpe/js/multiwan.js": 69,
        "../../cpe/js/wifi.js": 71,
        "../js/bee-custom.js": 77,
        "../js/bee-quick-lang.js": 79,
        "data_utility.js": 4,
        "dom-maker.js": 5,
        "event-emitter.js": 8,
        "form.js": 10,
        "navi.js": 16,
        "system.js": 23,
        "twz.js": 24,
        "virtual-dom.js": 26
    } ],
    111: [ function(d, t, e) {
        "use strict";
        var p = d("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = d("../js/bee-quick-lang.js").lang, i = d("event-emitter.js").EventEmiter, s = n(), n = e.attr, e = n.text || "", n = n.name || "", o = {}, r = {}, a = !0, l = !1, c = this.exports = {}, u = new i();
            this.obj = {
                created: function() {
                    function e() {
                        0 == o.el.e.value.length ? (r.el.e.style.display = "block", r.el.set(s.qerror.field_empty), 
                        a = !1) : /^\w*$/.test(o.el.e.value) ? (a = !0, r.el.set(s.qerror.goto_configured), 
                        r.el.e.style.display = "none") : (r.el.e.style.display = "block", r.el.set(s.qerror.field_invalid), 
                        a = !1);
                    }
                    c.on = function(t, e) {
                        return u.on(t, e);
                    }, c.is_valid = function() {
                        return a;
                    }, c.is_changed = function() {
                        return l;
                    }, c.get_value = function() {
                        return o.el.e.value;
                    }, c.set_value = function(t) {
                        return o.el.e.value = t;
                    }, c.disabled = function(t) {
                        o.el.disabled(t);
                    }, r.el.e.style.display = "none", c.changed = function() {
                        l = !0, e(), u.emit("change", o.el.e.value);
                    }, c.no_changed = function() {
                        l = !1;
                    }, o.el.on("input", function(t) {
                        l = !0, e(), u.emit("change", o.el.e.value);
                    });
                }
            }, this.tree = new p("div", {}), this.tree.root().set_class("quick-label bee-margin-bottom-10-px").child("div", {}).set_class("quick-label bee-qconf-line").child("label", {
                text: e
            }).set_class("bee-qconf-col1").up().child("input", {
                name: n,
                type: "text"
            }).set_class("bee-qconf-col2").bind(o).directive("bind", o).up().up().child("span", {}).set_class("bee-warning").child("label", {
                text: s.qerror.field_empty
            }).set_class("bee-qconf-field-error").bind(r).directive("bind", r).up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    112: [ function(h, t, e) {
        "use strict";
        var _ = h("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = h("../js/bee-quick-lang.js").lang, i = h("event-emitter.js").EventEmiter, s = h("system.js").rpc, n = n(), o = (e.attr, 
            {}), r = {}, a = {}, l = {}, c = [ o, r, a, l ], u = new i();
            function d() {
                u.emit("change", [ o.el.e.value, r.el.e.value, a.el.e.value, l.el.e.value ]);
            }
            var p = this.exports = {};
            this.obj = {
                created: function() {
                    c.forEach(function(t) {
                        t.el.on("click", d);
                    }), s("lan_get_total", {}).then(function(t) {
                        2 == t.total && (a.el.show(!1), l.el.show(!1));
                    });
                    var e = !1;
                    p.on = function(t, e) {
                        return u.on(t, e);
                    }, u.on("change", function(t) {
                        e = !0;
                    }), p.get_value = function() {
                        return [ o.el.e.checked, r.el.e.checked ];
                    }, p.is_valid = function() {
                        return !0;
                    }, p.is_changed = function() {
                        return e;
                    }, p.no_changed = function() {
                        e = !1;
                    }, p.set_value = function(t) {
                        o.el.e.checked = t[0], r.el.e.checked = t[1];
                    };
                }
            }, this.tree = new _("div", {}), this.tree.root().set_class("quick-label bee-qconf-group-tv").child("label", {
                text: n.qsettings.tv
            }).set_class("bee-qconfig-label").up().child("label", {
                text: n.qsettings.tv_text
            }).set_class("quick-label").up().child("div", {}).set_class("quick-label bee-qconf-line bee-qconf-tv").child("label", {}).set_class("quick-label container margin-left-0").child("input", {
                name: "lan1",
                type: "checkbox",
                checked: "checked"
            }).bind(o).directive("bind", o).up().child("span", {}).set_class("quick-label checkmark").up().child("span", {}).text("LAN1").up().up().child("label", {}).set_class("quick-label container").child("input", {
                name: "lan2",
                type: "checkbox",
                checked: "checked"
            }).bind(r).directive("bind", r).up().child("span", {}).set_class("quick-label checkmark").up().child("span", {}).text("LAN2").up().up().child("label", {}).set_class("quick-label container").bind(a).directive("bind", a).child("input", {
                name: "lan3",
                type: "checkbox",
                checked: "checked"
            }).bind(a).directive("bind", a).up().child("span", {}).set_class("quick-label checkmark").up().child("span", {}).set_class("quick-label").text("LAN3").up().up().child("label", {}).set_class("quick-label container").bind(l).directive("bind", l).child("input", {
                name: "lan4",
                type: "checkbox",
                checked: "checked"
            }).bind(l).directive("bind", l).up().child("span", {}).set_class("quick-label checkmark").up().child("span", {}).text("LAN4").up().up().up();
        };
    }, {
        "../js/bee-quick-lang.js": 79,
        "event-emitter.js": 8,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    113: [ function(s, t, e) {
        "use strict";
        var o = s("system.js").$, n = (s("system.js").login_rpc, s("virtual-dom.js").get_component), r = (s("virtual-dom.js").RenderMachine, 
        s("virtual-dom.js").ComponentHub, s("../../cpe/js/multiwan.js").Multiwan, s("../../cpe/js/cpe.js").cpe, 
        s("twz.js").twz), a = (s("../../luna-quick-menu/index.js"), s("../../../../autoconf.json")), l = {
            SCREEN_1_2_NO_WAN: 1,
            SCREEN_1_3_NO_WAN: 2,
            SCREEN_1_4_WAITING_60S: 3,
            SCREEN_1_5_WAITING: 4
        };
        function c(t, e, n, i) {
            this.app = t, this.place = e, this.RM = n, this.hub = i, this.status = "", this.blocked = !1;
        }
        c.prototype.stop = function() {
            this.app.show(!0), this.twz && (this.status = "", this.twz.el.show(!1));
        }, c.prototype.show_page = function(t) {
            if (!this.hub.search(e = "twz-code-" + t)) return console.error("twz notimplemented for code:" + t), 
            !1;
            var e = this.RM.render_component(e);
            return this.place.set(), this.RM.mount_in_dom(e, this.place), this.twz = n(e), this.app.show(!1), 
            !0;
        };
        var u = {};
        t.exports.twz = r, t.exports.twz_init = function(t, e, n) {
            "RTC" == a.defines.CONFIG_CUSTOMER ? (e.registry("twz-code-" + l.SCREEN_1_2_NO_WAN, s("first-start-nowan-1_2.vd").Ctor), 
            e.registry("twz-code-" + l.SCREEN_1_3_NO_WAN, s("first-start-nowan-1_3.vd").Ctor), 
            e.registry("twz-code-" + l.SCREEN_1_4_WAITING_60S, s("first-start-wait-1_4.vd").Ctor), 
            e.registry("twz-code-" + l.SCREEN_1_5_WAITING, s("first-start-wait-1_5.vd").Ctor)) : "BEELINE" == a.defines.CONFIG_CUSTOMER && e.registry("twz-code-" + l.SCREEN_1_2_NO_WAN, s("../../luna-quick-menu/vd/bee-twz-no-wan.vd").Ctor);
            var i = o.div("twz");
            o.body().add(i), u.luna_twz = new c(t, i, n, e), r().on("twz-active", function(t) {
                return u.luna_twz.show_page(r().code);
            }).on("twz-stop", function(t) {
                return u.luna_twz.stop();
            }).set_code_converter(function(t) {
                return console.log(t), "twz-active";
            }).start();
        }, t.exports.TWZ_STATUS = l;
    }, {
        "../../../../autoconf.json": 1,
        "../../cpe/js/cpe.js": 67,
        "../../cpe/js/multiwan.js": 69,
        "../../luna-quick-menu/index.js": 76,
        "../../luna-quick-menu/vd/bee-twz-no-wan.vd": 99,
        "first-start-nowan-1_2.vd": 114,
        "first-start-nowan-1_3.vd": 115,
        "first-start-wait-1_4.vd": 116,
        "first-start-wait-1_5.vd": 117,
        "system.js": 23,
        "twz.js": 24,
        "virtual-dom.js": 26
    } ],
    114: [ function(o, t, e) {
        "use strict";
        var r = o("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = o("multilang.js").lang, i = o("twz.js").twz, n = n(), s = {};
            this.exports = {};
            this.obj = {
                mounted: function() {
                    s.el.on("click", function(t) {
                        i().stop();
                    });
                }
            }, this.tree = new r("div", {}), this.tree.root().set_class("nowan_container").text("\n    ").child("div", {}).set_class("logo").text("\n      ").child("img", {
                id: "logo",
                src: "topbar.png",
                border: "0"
            }).up().text("\n    ").up().text("\n    ").child("div", {}).set_class("nowan_desc").text("\n      ").child("ul", {}).set_class("nowan_desc-list").text("\n        ").child("li", {}).set_class("desc-list_item").text("\n          ").child("span", {
                text: n.wizard.nowan_warning
            }).set_class("desc_list-title").up().text("\n        ").up().text("\n        ").child("li", {}).set_class("desc-list_item").text("\n          ").child("span", {
                text: n.wizard.nowan_text_rostel
            }).set_class("desc_list-text").up().text("\n        ").up().text("\n        ").child("li", {}).set_class("desc-list_item desc_img").text("\n          ").child("img", {
                id: "wanPicture",
                src: "WAN.gif",
                border: "0"
            }).up().text("\n        ").up().text("\n      ").up().text("\n    ").up().text("\n\n    ").child("div", {}).set_class("nowan_link").text("\n      ").child("input", {
                value: n.button.start,
                type: "button"
            }).set_class("link_bg-btn").bind(s).directive("bind", s).up().text("\n      ").child("input", {
                value: n.button.manual,
                type: "button"
            }).set_class("link_bg-btn").bind(s).directive("bind", s).up().text("\n    ").up().text("\n  ");
        };
    }, {
        "multilang.js": 15,
        "twz.js": 24,
        "virtual-dom.js": 26
    } ],
    115: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("multilang.js").lang, i = r("twz.js").twz, s = n(), n = {}, o = {};
            this.exports = {};
            this.obj = {
                mounted: function() {
                    o.el.on("click", function(t) {
                        i().stop();
                    });
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("nowan_container").text("\n    ").child("div", {}).set_class("logo").text("\n      ").child("img", {
                id: "logo",
                src: "topbar.png",
                border: "0"
            }).up().text("\n    ").up().text("\n    ").child("div", {}).set_class("nowan_desc").text("\n      ").child("ul", {}).set_class("nowan_desc-list").text("\n        ").child("li", {}).set_class("desc-list_item").text("\n          ").child("span", {
                text: s.wizard.nowan_warning
            }).set_class("desc_list-title").up().text("\n        ").up().text("\n        ").child("li", {}).set_class("desc-list_item").text("\n          ").child("span", {
                text: s.wizard.nowan_desc
            }).set_class("desc_list-text").up().text("\n        ").up().text("\n        ").child("li", {}).set_class("desc-list_item desc_img").text("\n          ").child("img", {
                id: "wanPicture",
                src: "WAN.gif",
                border: "0"
            }).up().text("\n        ").up().text("\n      ").up().text("\n    ").up().text("\n\n    ").child("div", {}).set_class("nowan_link").text("\n      ").child("input", {
                value: s.button.manual,
                type: "button"
            }).set_class("link_bg-btn").bind(o).directive("bind", o).up().text("\n      ").child("input", {
                value: s.button.next,
                id: "next",
                type: "button"
            }).set_class("link_bg-btn").bind(n).directive("bind", n).up().text("\n    ").up().text("\n  ");
        };
    }, {
        "multilang.js": 15,
        "twz.js": 24,
        "virtual-dom.js": 26
    } ],
    116: [ function(c, t, e) {
        "use strict";
        var u = c("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = c("multilang.js").lang, i = (c("event-emitter.js").EventEmiter, c("twz.js").twz, 
            c("../../luna-wizard/lib/js/wizard.js").wizard, c("../../../lib/js/static-info.js").no_login_static_info, 
            c("../../../lib/js/system.js").no_login_rpc), n = (c("../../../lib/js/static-info.js").update, 
            n());
            n.login = "login", n.password = "password", n.next = "next", n.back = "back", n.save = "save file", 
            n.warning.wifi = n.warning.wifi || {};
            var s = {}, o = e.attr.time || 60, r = !1, a = {};
            function l() {
                i("no_login_rpc_apmib_get", {
                    list: [ "configured" ]
                }).then(function(t) {
                    a.static_info = t, r = "false" !== t.configured;
                }).then(function() {
                    var t;
                    r ? window.location.href = "http://rt.ru/" : 0 < o ? (o--, s.el.set(o), t = setTimeout(l, 1e3)) : (console.log("timer ended, go to wizard"), 
                    clearTimeout(t), app().navi().go("wizard"));
                });
            }
            this.obj = {
                mounted: function() {
                    l();
                }
            }, this.tree = new u("div", {}), this.tree.root().set_class("wait_container").text("\n      ").child("div", {}).set_class("logo").text("\n          ").child("img", {
                id: "logo",
                src: "topbar.png",
                border: "0"
            }).up().text("\n      ").up().text("\n      ").child("div", {}).set_class("wait_wrapper").text("\n          ").child("div", {}).set_class("wait_desc").text("\n            ").child("ul", {}).set_class("wait_desc-list").text("\n              ").child("li", {}).set_class("wait_list-item").text("\n                ").child("p", {
                text: n.wizard.apply_descr
            }).set_class("wait_list-text").up().text("\n              ").up().text("\n              ").child("li", {}).set_class("wait_list-item").text("\n                ").child("p", {}).set_class("wait_desc-timer").bind(s).directive("bind", s).up().text("\n              ").up().text("\n              ").child("li", {}).set_class("wait_list-item wait_warning").text("\n                ").child("p", {
                text: n.wizard.apply_warning
            }).set_class("wait_list-text text_red").up().text("\n              ").up().text("\n            ").up().text("\n          ").up().text("\n      ").up().text("\n  ");
        };
    }, {
        "../../../lib/js/static-info.js": 22,
        "../../../lib/js/system.js": 23,
        "../../luna-wizard/lib/js/wizard.js": 123,
        "event-emitter.js": 8,
        "multilang.js": 15,
        "twz.js": 24,
        "virtual-dom.js": 26
    } ],
    117: [ function(i, t, e) {
        "use strict";
        var s = i("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            (0, i("multilang.js").lang)();
            var n = this.exports = {};
            this.obj = {
                mounted: function() {
                    n.show = function() {};
                }
            }, this.tree = new s("div", {}), this.tree.root().set_class("wait_container").child("div", {}).set_class("logo").child("img", {
                id: "logo",
                src: "topbar.png",
                border: "0"
            }).up().up().child("div", {}).set_class("wait_desc").child("ul", {}).set_class("wait_desc-list").child("li", {}).set_class("wait_list-item").child("span", {}).set_class("wait_list-text").text(" ?????????¶????N??µ. ?˜???µN? ???°N?N?N????????° N?N?N?N?????N?N????°!").up().up().child("li", {}).set_class("wait_list-item").child("span", {}).set_class("wait_list-text text_red").text(" ???µ ??N????»N?N??°??N??µ ????N??°?????µ N?N?N?N?????N?N????°!").up().up().child("li", {}).set_class("wait_list-item wait_img").child("img", {
                width: "50px",
                height: "50px",
                src: "/Spin.gif",
                id: "wan_img"
            }).up().up().up().up();
        };
    }, {
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    118: [ function(e, t, n) {
        "use strict";
        var i = e("virtual-dom.js").RenderMachine, s = e("virtual-dom.js").ComponentHub, o = e("wizard.js").wizard, r = e("../basic-components/index.js");
        function a() {
            var t = this._hub = new s();
            t.registry("profiles", e("profiles.vd").Ctor), t.registry("wifi", e("wifi.vd").Ctor), 
            t.registry("tv", e("tv.vd").Ctor), t.registry("voip", e("voip.vd").Ctor), t.registry("vlan-tag", e("tag.vd").Ctor), 
            t.registry("bridge", e("bridge.vd").Ctor), t.registry("PPPoE", e("PPPoE.vd").Ctor), 
            t.registry("pppoe_fail", e("pppoe_fail.vd").Ctor), t.registry("pppoe_again", e("pppoe_again.vd").Ctor), 
            t.registry("confirm", e("confirm.vd").Ctor), t.registry("applying", e("applying.vd").Ctor), 
            t.registry("done", e("done.vd").Ctor), t.registry("nowan", e("nowan.vd").Ctor), 
            r.registry(t), this.RM = new i(t);
        }
        a.prototype.run = function(t, e) {
            o().run("app", this.RM, e);
        };
        var l = {};
        t.exports.wizard_package = function() {
            return l._wiz || (l._wiz = new a()), l._wiz;
        };
    }, {
        "../basic-components/index.js": 49,
        "PPPoE.vd": 124,
        "applying.vd": 125,
        "bridge.vd": 126,
        "confirm.vd": 127,
        "done.vd": 128,
        "nowan.vd": 129,
        "pppoe_again.vd": 130,
        "pppoe_fail.vd": 131,
        "profiles.vd": 132,
        "tag.vd": 133,
        "tv.vd": 134,
        "virtual-dom.js": 26,
        "voip.vd": 135,
        "wifi.vd": 136,
        "wizard.js": 123
    } ],
    119: [ function(t, e, n) {
        "use strict";
        function i() {
            this.stages = {}, this._history = [], this._back = !1, this.max_steps = 5e3, this.current_num = 0;
        }
        i.prototype.stage = function(t, e) {
            return this.stages[t] = e, this;
        }, i.prototype.history = function(t) {
            return this._history;
        }, i.prototype.back = function() {
            this._history.pop(), this.next_stage = this._history.pop();
        }, i.prototype.go = function(t) {
            return this.next_stage = t, this;
        }, i.prototype.is_end = function(t) {
            return "end" == this.next_stage;
        }, i.prototype.run = function() {
            if (this.current_num++, this.current_num > this.max_steps) return console.error("Steps is too many(>5000"), 
            this.next_stage = "end", !0;
            if (!this.stages[this.next_stage]) return console.error(this.next_stage + " is not found"), 
            this.next_stage = "end", !0;
            var t = this.next_stage;
            return this._history.push(t), this.next_stage = "end", this.stages[t](this);
        }, i.prototype.start_async = function() {
            this.next_stage = "begin";
            var n = this;
            function i() {
                return n.is_end() ? n._resolve() : n.run().then(i);
            }
            return new Promise(function(t, e) {
                n._resolve = t, n._reject = e, i();
            });
        }, i.prototype.start = function() {
            for (this.next_stage = "begin"; !this.is_end(); ) this.run();
            return this;
        }, e.exports.Flow = i;
    }, {} ],
    120: [ function(t, e, n) {
        "use strict";
        function i(t, e) {
            this.region = t, this.branchNo = e;
        }
        function s(t, e, n) {
            this.region = t, this.branchName = e, this.profileNo = n;
        }
        function o(t, e, n, i, s, o, r, a, l, c, u, d, p, h, _, f, m) {
            this.branchName = t, this.serviceName = e, this.service = n, this.PVC1 = i, this.PVC2 = s, 
            this.PVC3 = o, this.PVC4 = r, this.PVC5 = a, this.interPortmap = l, this.STBPortmap = c, 
            this.VoIPPortmap = u, this.dhcpStatus = d, this.aclStatus = p, this.wanport = h, 
            this.tr069 = _, this.ondemandSec = f, this.specialDV = m;
        }
        var r = new Array(), a = new Array(), l = new Array(), c = "UNTAG_B", u = "TAG_DYN_4_B", d = "TAG_DYN_5_B", p = "TAG_DYN_6_B", h = "TAG_DYN_7_B", _ = "TAG_550_3_B", f = "TAG_50_4_B", m = "TAG_500_4_B", v = "TAG_397_4_B", b = "TAG_130_5_B", g = "TAG_1101_5_B", x = "TAG_5_4_B  ", w = "TAG_16_5_B", y = "TAG_3001_6_B", j = "TAG_999_4_B", k = "TAG_40_4_B", N = "TAG_3530_4_B", I = "TAG_3539_4_B", E = "TAG_101_4_B", P = "TAG_415_5_B", A = "TAG_40_5_B3", C = "TAG_400_5_B", D = "TAG_1299_5_B", L = "TAG_1340_6_B", T = "TAG_34_4_B", q = "TAG_4093_4_B", S = "TAG_4092_7_B", O = "PPPOE_UNTAG_NAT", R = "PPPOE_TAG_DYN_NAT", F = "PPPOE_TAG_DYN_1_NAT", M = "IPOE_UNTAG_NAT", G = "PPPOE_IP6_TAG_DYN_NAT", U = "PPPOE_IP6_UNTAG_NAT", V = "PPPOE_TAG_310_0_NAT", W = "PPPOE_TAG_20_0_NAT", z = "PPPOE_TAG_16_0_NAT", B = "PPPOE_UNTAG_NAT_IGMP", H = "IPOE_UNTAG_NAT_IGMP", $ = "IPOE_TR069_UNTAG", Y = "IPOE_TR069_TAG_4040_7", X = 1, Q = 10, J = 100, Z = 0;
        r[Z++] = new i("?????»???°", 14), r[Z++] = new i("???°?»N??????? ????N?N?????", 1), r[Z++] = new i("?¦?µ??N?N?", 2), 
        r[Z++] = new i("???µ???µN???-???°???°??", 1), r[Z++] = new i("?????±??N?N?", 14), r[Z++] = new i("??N??°?»", 9), 
        r[+Z] = new i("?®??", 1), Z = 0, a[Z++] = new s("?????»???°", "????N?????N???????", 5), a[Z++] = new s("?????»???°", "? ?µN???N??±?»?????° ???°N????? ?­?»", 5), 
        a[Z++] = new s("?????»???°", "? ?µN???N??±?»?????° ????N?????????N?", 5), a[Z++] = new s("?????»???°", "??N??µ???±N?N???N???????", 4), 
        a[Z++] = new s("?????»???°", "?????¶?µ????N?????N???????", 5), a[Z++] = new s("?????»???°", "???µ???·?µ??N???????", 5), 
        a[Z++] = new s("?????»???°", "???°???°N?N???????", 3), a[Z++] = new s("?????»???°", "???°N??°N?????N???????", 5), 
        a[Z++] = new s("?????»???°", "? ?µN???N??±?»?????° ???°N??°N?N?N??°?? (???°?·?°??N?)", 2), a[Z++] = new s("?????»???°", "? ?µN???N??±?»?????° ???°N??°N?N?N??°?? (???°?±?µN??µ?¶??N??µ ?§?µ?»??N?)", 2), 
        a[Z++] = new s("?????»???°", "??????N?N?N?N????°N? ? ?µN???N??±?»?????°", 5), a[Z++] = new s("?????»???°", "???»N?N???????N???????", 1), 
        a[Z++] = new s("?????»???°", "?§N????°N?N????°N? ? ?µN???N??±?»?????°(PPPoE)", 5), a[Z++] = new s("?????»???°", "?§N????°N?N????°N? ? ?µN???N??±?»?????°(DHCP)", 5), 
        a[Z++] = new s("???°?»N??????? ????N?N?????", "??N??µ N????»???°?»N?", 5), a[Z++] = new s("?¦?µ??N?N?", "Onlime (??. ????N??????°) ?? QWERTY", 1), 
        a[Z++] = new s("?¦?µ??N?N?", "??N??µ N????»???°?»N?A ", 1), a[Z++] = new s("???µ???µN???-???°???°??", "??N??µ N????»???°?»N?A A A ", 3), 
        a[Z++] = new s("?????±??N?N?", "???µ???µN?????N???????", 3), a[Z++] = new s("?????±??N?N?", "??????????N??·???µN???", 3), 
        a[Z++] = new s("?????±??N?N?", "??????????N??·???µN??? (N??µ?»?°)", 3), a[Z++] = new s("?????±??N?N?", "??N??°N?????N?N?N???????, ???¦??", 2), 
        a[Z++] = new s("?????±??N?N?", "??????N???????", 1), a[Z++] = new s("?????±??N?N?", "?˜N???N?N?N???????", 2), 
        a[Z++] = new s("?????±??N?N?", "??N??°N?????N?N?N???????(1)", 3), a[Z++] = new s("?????±??N?N?", "??N??°N?????N?N?N???????(2)", 3), 
        a[Z++] = new s("?????±??N?N?", "????????N????±??N?N???????(????N?????)", 3), a[Z++] = new s("?????±??N?N?", "??N?N?N?N?N???????", 2), 
        a[Z++] = new s("?????±??N?N?", "???»N??°??N???????, ????N?????-???»N??°??N??????? ?¦??", 4), a[Z++] = new s("?????±??N?N?", "????N???????", 2), 
        a[Z++] = new s("?????±??N?N?", "??N?N?N?N?N??????? (???¦??)", 2), a[Z++] = new s("??N??°?»", "??N??µ N????»???°?»N?A A A A ", 5), 
        a[Z++] = new s("?®??", "??N??µ N????»???°?»N?A A ", 5), a[+Z] = new s("?????±??N?N?", "????????N????±??N?N???????(???±?»?°N?N?N?)", 3), 
        Z = 0, l[Z++] = new o("????N?????N???????", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????N?????N???????", "?˜??N??µN??°??N??????????µ ????", 1, 0, c, 0, 0, 0, 0, 14, 0, 0, X, 80, 0, 0, 0), 
        l[Z++] = new o("????N?????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, c, 0, 0, 0, 14, 1, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????N?????N???????", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, O, 0, c, 0, 0, 14, 0, 1, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????N?????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, c, c, 0, 0, 12, 1, 2, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ???°N????? ?­?»", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ???°N????? ?­?»", "?˜??N??µN??°??N??????????µ ????", 1, 0, d, 0, 0, 0, 0, 14, 0, 0, X, 80, 0, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ???°N????? ?­?»", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, d, 0, 0, 0, 14, 1, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ???°N????? ?­?»", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, R, 0, h, 0, 0, 7, 0, 8, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ???°N????? ?­?»", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, R, d, h, 0, 0, 6, 1, 8, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ????N?????????N?", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ????N?????????N?", "?˜??N??µN??°??N??????????µ ????", 1, 0, _, 0, 0, 0, 0, 14, 0, 0, 0, 80, 0, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ????N?????????N?", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, F, _, 0, 0, 0, 14, 1, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ????N?????????N?", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, F, 0, P, 0, 0, 7, 0, 8, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ????N?????????N?", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, F, _, P, 0, 0, 6, 1, 8, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ???±N?N???N???????", "???????°N??????? ?˜??N??µN????µN?", 0, M, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ???±N?N???N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, M, c, 0, 0, 0, 14, 1, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ???±N?N???N???????", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, M, 0, c, 0, 0, 14, 0, 1, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ???±N?N???N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, M, c, c, 0, 0, 12, 1, 2, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?????¶?µ????N?????N???????", "???????°N??????? ?˜??N??µN????µN?", 0, G, 0, 0, 0, 0, 15, 0, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("?????¶?µ????N?????N???????", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, G, 0, d, 0, 0, 14, 0, 4, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("?????¶?µ????N?????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, G, u, 0, 0, 0, 7, 8, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("?????¶?µ????N?????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, G, u, d, 0, 0, 3, 8, 4, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("?????¶?µ????N?????N???????", "?˜??N??µN??°??N??????????µ ????", 1, 0, u, 0, 0, 0, 14, 0, 4, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("???µ???·?µ??N???????", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???µ???·?µ??N???????", "?˜??N??µN??°??N??????????µ ????", 1, 0, c, 0, 0, 0, 0, 14, 0, 0, X, 80, 0, 0, 0), 
        l[Z++] = new o("???µ???·?µ??N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, f, 0, 0, 0, 14, 1, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???µ???·?µ??N???????", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, O, 0, A, 0, 0, 7, 0, 8, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???µ???·?µ??N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, f, A, 0, 0, 6, 1, 8, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???°???°N?N???????", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???°???°N?N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, m, 0, 0, 0, 13, 2, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???°???°N?N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, m, C, 0, 0, 3, 2, 4, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???°N??°N?????N???????", "???????°N??????? ?˜??N??µN????µN?", 0, U, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???°N??°N?????N???????", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, U, 0, d, 0, 0, 14, 0, 4, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???°N??°N?????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, U, u, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???°N??°N?????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, U, u, d, 0, 0, 3, 8, 4, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???°N??°N?????N???????", "?˜??N??µN??°??N??????????µ ????", 1, 0, u, 0, 0, 0, 0, 15, 0, 0, X, 80, 0, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ???°N??°N?N?N??°?? (???°?·?°??N?)", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 0, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ???°N??°N?N?N??°?? (???°?·?°??N?)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, v, 0, 0, 0, 14, 1, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ???°N??°N?N?N??°?? (???°?±?µN??µ?¶??N??µ ?§?µ?»??N?)", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 0, 0, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("? ?µN???N??±?»?????° ???°N??°N?N?N??°?? (???°?±?µN??µ?¶??N??µ ?§?µ?»??N?)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, b, 0, 0, 0, 14, 1, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("??????N?N?N?N????°N? ? ?µN???N??±?»?????°", "???????°N??????? ?˜??N??µN????µN?", 0, M, 0, 0, 0, 0, 15, 0, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("??????N?N?N?N????°N? ? ?µN???N??±?»?????°", "?˜??N??µN??°??N??????????µ ????", 1, 0, c, 0, 0, 0, 0, 14, 0, 0, 0, 80, 0, 0, 0), 
        l[Z++] = new o("??????N?N?N?N????°N? ? ?µN???N??±?»?????°", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, M, 0, c, 0, 0, 7, 0, 8, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("??????N?N?N?N????°N? ? ?µN???N??±?»?????°", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, M, c, 0, 0, 0, 14, 4, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("??????N?N?N?N????°N? ? ?µN???N??±?»?????°", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, M, c, c, 0, 0, 3, 4, 8, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("???»N?N???????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 0, H, 0, 0, 0, 0, 0, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(PPPoE)", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(PPPoE)", "?˜??N??µN??°??N??????????µ ????", 1, 0, c, 0, 0, 0, 0, 14, 0, 0, X, 80, 0, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(PPPoE)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, c, 0, 0, 0, 14, 1, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(PPPoE)", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 0, O, 0, c, 0, 0, 7, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(PPPoE)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, c, c, 0, 0, 6, 1, 8, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(DHCP)", "???????°N??????? ?˜??N??µN????µN?", 0, M, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(DHCP)", "?˜??N??µN??°??N??????????µ ????", 1, 0, c, 0, 0, 0, 0, 14, 0, 0, X, 80, 0, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(DHCP)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, M, c, 0, 0, 0, 14, 1, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(DHCP)", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 0, M, 0, 0, 0, 0, 7, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?§N????°N?N????°N? ? ?µN???N??±?»?????°(DHCP)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, M, c, c, 0, 0, 6, 1, 8, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, $, 15, 0, 0, 0, 0, 80, 5, 0, 1), 
        l[Z++] = new o("??N??µ N????»???°?»N?", "?˜??N??µN??°??N??????????µ ????", 1, 0, c, 0, 0, $, 0, 14, 0, 0, 0, 80, 5, 0, 1), 
        l[Z++] = new o("??N??µ N????»???°?»N?", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, c, 0, 0, $, 7, 8, 0, 0, 0, 80, 5, 0, 1), 
        l[Z++] = new o("??N??µ N????»???°?»N?", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, O, 0, c, 0, $, 11, 0, 4, 0, 0, 80, 5, 0, 1), 
        l[Z++] = new o("??N??µ N????»???°?»N?", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, c, c, 0, $, 3, 8, 4, 0, 0, 80, 5, 0, 1), 
        l[Z++] = new o("Onlime (??. ????N??????°) ?? QWERTY", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 0, H, 0, 0, 0, 0, 0, 0, 0, 0, 0, 161, 1, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A ", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 0, B, 0, 0, 0, 0, 0, 0, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A A ", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, $, 0, 15, 0, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A A ", "?˜??N??µN??°??N??????????µ ????", 1, 0, c, 0, $, 0, 0, 14, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A A ", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, c, 0, $, 0, 7, 8, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("???µ???µN?????N???????", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 0, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???µ???µN?????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, d, p, 0, 0, 14, 4, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???µ???µN?????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, d, p, 0, 0, 3, 4, 8, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??????????N??·???µN???", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, Y, 0, 0, 0, 0, J + X, 80, 5, 0, 0), 
        l[Z++] = new o("??????????N??·???µN???", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, B, D, 0, 0, Y, 7, 8, 0, 0, J + X, 80, 5, 0, 0), 
        l[Z++] = new o("??????????N??·???µN???", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, B, D, L, 0, Y, 7, 8, 8, 0, J + X, 80, 5, 0, 0), 
        l[Z++] = new o("??????????N??·???µN??? (N??µ?»?°)", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, Y, 15, 0, 0, 0, X, 80, 5, 0, 0), 
        l[Z++] = new o("??????????N??·???µN??? (N??µ?»?°)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, B, T, 0, 0, Y, 7, 8, 0, 0, X, 80, 5, 0, 0), 
        l[Z++] = new o("??????????N??·???µN??? (N??µ?»?°)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, B, T, L, 0, Y, 3, 8, 4, 0, X, 80, 5, 0, 0), 
        l[Z++] = new o("??N??°N?????N?N?N???????, ???¦??", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??°N?????N?N?N???????, ???¦??", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, g, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??????N???????", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?˜N???N?N?N???????", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?˜N???N?N?N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, x, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??°N?????N?N?N???????(1)", "???????°N??????? ?˜??N??µN????µN?", 0, R, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??°N?????N?N?N???????(1)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, R, w, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??°N?????N?N?N???????(1)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, R, w, h, 0, 0, 3, 8, 4, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??°N?????N?N?N???????(2)", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??°N?????N?N?N???????(2)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, w, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??°N?????N?N?N???????(2)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, w, h, 0, 0, 3, 8, 4, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????????N????±??N?N???????(????N?????)", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????????N????±??N?N???????(????N?????)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, R, q, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????????N????±??N?N???????(????N?????)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, R, q, S, 0, 0, 3, 8, 4, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N?N?N?N?N???????", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N?N?N?N?N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, c, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???»N??°??N???????, ????N?????-???»N??°??N??????? ?¦??", "???????°N??????? ?˜??N??µN????µN?", 0, M, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???»N??°??N???????, ????N?????-???»N??°??N??????? ?¦??", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, M, c, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???»N??°??N???????, ????N?????-???»N??°??N??????? ?¦??", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, M, 0, y, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("???»N??°??N???????, ????N?????-???»N??°??N??????? ?¦??", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, M, c, y, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, c, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????N???????", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N?N?N?N?N??????? (???¦??)", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N?N?N?N?N??????? (???¦??)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, c, c, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("?????°N??µN??????±N?N???N???????", "???????°N??????? ?˜??N??µN????µN?", 0, U, 0, 0, $, 0, 15, 0, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°N??µN??????±N?N???N???????", "?˜??N??µN??°??N??????????µ ????", 1, 0, u, 0, $, 0, 0, 15, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°N??µN??????±N?N???N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, U, u, 0, $, 0, 7, 8, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°N??µN??????±N?N???N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, U, u, d, $, 0, 3, 8, 4, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°N??µN??????±N?N???N???????", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, U, 0, d, $, 0, 14, 0, 4, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("??N?N????°??N???????", "???????°N??????? ?˜??N??µN????µN?", 0, V, 0, 0, $, 0, 15, 0, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("??N?N????°??N???????", "?˜??N??µN??°??N??????????µ ????", 1, 0, j, 0, $, 0, 0, 7, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("??N?N????°??N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, V, j, 0, $, 0, 14, 1, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("???µN???N???????", "???????°N??????? ?˜??N??µN????µN?", 0, W, 0, 0, $, 0, 15, 0, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("???µN???N???????", "?˜??N??µN??°??N??????????µ ????", 1, 0, k, 0, $, 0, 0, 7, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("???µN???N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, W, k, 0, $, 0, 14, 1, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("??N????µ??N???????", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, $, 0, 15, 0, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("??N????µ??N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, u, 0, $, 0, 7, 8, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("??N????µ??N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, u, d, $, 0, 3, 8, 4, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("???°??N?N?-???°??N?????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, R, c, 0, $, 0, 14, 1, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?§?µ?»N??±????N???????", "???????°N??????? ?˜??N??µN????µN?", 0, G, 0, 0, $, 0, 15, 0, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?§?µ?»N??±????N???????", "?˜??N??µN??°??N??????????µ ????", 1, 0, u, 0, $, 0, 0, 7, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?§?µ?»N??±????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, G, u, 0, $, 0, 7, 8, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?§?µ?»N??±????N???????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, G, u, d, $, 0, 3, 8, 4, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?§?µ?»N??±????N???????", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, G, 0, d, $, 0, 14, 0, 4, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°?»??-???µ???µN??????? N????»???°?» ????????N?N??µ????????N??????? ? ????", "???????°N??????? ?˜??N??µN????µN?", 0, R, 0, 0, $, 0, 15, 0, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°?»??-???µ???µN??????? N????»???°?» ????????N?N??µ????????N??????? ? ????", "?˜??N??µN??°??N??????????µ ????", 1, 0, N, 0, $, 0, 0, 7, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°?»??-???µ???µN??????? N????»???°?» ????????N?N??µ????????N??????? ? ????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, R, N, 0, $, 0, 14, 1, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°?»??-???µ???µN??????? N????»???°?» ????N??±N?N?N??????? ? ????", "???????°N??????? ?˜??N??µN????µN?", 0, R, 0, 0, $, 0, 15, 0, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°?»??-???µ???µN??????? N????»???°?» ????N??±N?N?N??????? ? ????", "?˜??N??µN??°??N??????????µ ????", 1, 0, I, 0, $, 0, 0, 7, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°?»??-???µ???µN??????? N????»???°?» ????N??±N?N?N??????? ? ????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, R, I, 0, $, 0, 14, 1, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°?»??-???µ???µN??????? N????»???°?» ???°?»?µN??°N???N??????? ? ????", "???????°N??????? ?˜??N??µN????µN?", 0, z, 0, 0, $, 0, 15, 0, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°?»??-???µ???µN??????? N????»???°?» ???°?»?µN??°N???N??????? ? ????", "?˜??N??µN??°??N??????????µ ????", 1, 0, E, 0, $, 0, 0, 7, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("?????°?»??-???µ???µN??????? N????»???°?» ???°?»?µN??°N???N??????? ? ????", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, z, E, 0, $, 0, 14, 1, 0, 0, 0, 80, 4, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A ", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 0, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A ", "?˜??N??µN??°??N??????????µ ????", 1, 0, c, 0, 0, 0, 0, 14, 0, 0, X, 80, 0, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A ", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, c, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A ", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, O, 0, c, 0, 0, 7, 0, 8, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A ", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, c, c, 0, 0, 3, 8, 4, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????????N????±??N?N???????(???±?»?°N?N?N?)", "???????°N??????? ?˜??N??µN????µN?", 0, O, 0, 0, 0, 0, 15, 0, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????????N????±??N?N???????(???±?»?°N?N?N?)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, O, c, 0, 0, 0, 7, 8, 0, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("????????N????±??N?N???????(???±?»?°N?N?N?)", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, O, c, c, 0, 0, 3, 8, 4, 0, X, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A A A ", "???????°N??????? ?˜??N??µN????µN?", 0, R, 0, 0, 0, 0, 15, 0, 0, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A A A ", "?˜??N??µN??°??N??????????µ ????", 1, 0, u, 0, 0, 0, 14, 0, 4, 0, 0, 80, 1, 0, 0), 
        l[Z++] = new o("??N??µ N????»???°?»N?A A A A ", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????", 3, R, u, 0, 0, 0, 7, 8, 0, 0, 0, 80, 1, 0, 0), 
        l[+Z] = new o("??N??µ N????»???°?»N?A A A A ", "???????°N??????? ?˜??N??µN????µN?+???µ?»?µN?????", 4, R, 0, d, 0, 0, 14, 0, 4, 0, 0, 80, 1, 0, 0), 
        l[141] = new o("??N??µ N????»???°?»N?A A A A ", "???????°N??????? ?˜??N??µN????µN?+?˜??N??µN??°??N??????????µ ????+???µ?»?µN?????", 5, R, u, d, 0, 0, 3, 8, 4, 0, 0, 80, 1, 0, 0);
        var K = {};
        function tt(t, e, n, i, s, o, r) {
            this.protocol = t, this.connType = e, this.vlanId = n, this.vlanPriority = i, this.nat = s, 
            this.droute = o, this.igmp = r;
        }
        function et() {
            l.forEach(function(t) {
                var e;
                t.acl = (e = t.aclStatus, t = {}, J <= e && (e -= J, t.web = !0), Q <= e && (e -= Q, 
                t.telnet = !0), X <= e && (e -= X, t.ping = !0), t);
            });
            var t = r.map(function(t) {
                return {
                    name: t.region,
                    size: t.branchNo
                };
            });
            return t.forEach(function(e) {
                var t = a.filter(function(t) {
                    return t.region == e.name;
                });
                e.subregions = t.map(function(t) {
                    return {
                        name: t.branchName,
                        size: t.profileNo
                    };
                }), e.subregions.forEach(function(e) {
                    var t = l.filter(function(t) {
                        return t.branchName == e.name;
                    });
                    e.profiles = t.map(function(t) {
                        return {
                            name: t.serviceName,
                            detail: t
                        };
                    });
                });
            }), t;
        }
        K[0] = new tt("", "", 0, 0, 0, 0, 0), K[O] = new tt("IPv4", "PPPoE", -1, 0, 1, 1, 0), 
        K[R] = new tt("IPv4", "PPPoE", -2, 0, 1, 1, 0), K[F] = new tt("IPv4", "PPPoE", -2, 1, 1, 1, 0), 
        K[M] = new tt("IPv4", "DHCP IPoE", -1, 0, 1, 1, 0), K[G] = new tt("IPv4+IPv6", "PPPoE", -2, 0, 1, 1, 0), 
        K[U] = new tt("IPv4+IPv6", "PPPoE", -1, 0, 1, 1, 0), K[V] = new tt("IPv4", "PPPoE", 310, 0, 1, 1, 0), 
        K[W] = new tt("IPv4", "PPPoE", 20, 0, 1, 1, 0), K[z] = new tt("IPv4", "PPPoE", 16, 0, 1, 1, 0), 
        K[c] = new tt("IPv4", "Bridge", -1, 0, 0, 0, 0), K[d] = new tt("IPv4", "Bridge", -2, 5, 0, 0, 0), 
        K[u] = new tt("IPv4", "Bridge", -2, 4, 0, 0, 0), K[h] = new tt("IPv4", "Bridge", -2, 7, 0, 0, 0), 
        K[p] = new tt("IPv4", "Bridge", -2, 6, 0, 0, 0), K[_] = new tt("IPv4", "Bridge", 550, 3, 0, 0, 0), 
        K[f] = new tt("IPv4", "Bridge", 50, 4, 0, 0, 0), K[m] = new tt("IPv4", "Bridge", 500, 4, 0, 0, 0), 
        K[v] = new tt("IPv4", "Bridge", 397, 4, 0, 0, 0), K[b] = new tt("IPv4", "Bridge", 130, 5, 0, 0, 0), 
        K[g] = new tt("IPv4", "Bridge", 1101, 5, 0, 0, 0), K[x] = new tt("IPv4", "Bridge", 5, 4, 0, 0, 0), 
        K[w] = new tt("IPv4", "Bridge", 16, 5, 0, 0, 0), K[y] = new tt("IPv4", "Bridge", 3001, 6, 0, 0, 0), 
        K[j] = new tt("IPv4", "Bridge", 999, 4, 0, 0, 0), K[k] = new tt("IPv4", "Bridge", 40, 4, 0, 0, 0), 
        K[N] = new tt("IPv4", "Bridge", 3530, 4, 0, 0, 0), K[I] = new tt("IPv4", "Bridge", 3539, 4, 0, 0, 0), 
        K[E] = new tt("IPv4", "Bridge", 101, 4, 0, 0, 0), K[P] = new tt("IPv4", "Bridge", 415, 5, 0, 0, 0), 
        K[A] = new tt("IPv4", "Bridge", 40, 5, 0, 0, 0), K[C] = new tt("IPv4", "Bridge", 400, 5, 0, 0, 0), 
        K.TAG_DYN_B3 = new tt("IPv4", "Bridge", -2, 0, 0, 0, 0), K[D] = new tt("IPv4", "Bridge", 1299, 5, 0, 0, 0), 
        K[Y] = new tt("IPv4", "DHCP IPoE", 4040, 7, 0, 0, 0), K[$] = new tt("IPv4", "DHCP IPoE", -1, 0, 0, 0, 0), 
        K[B] = new tt("IPv4", "PPPoE", -1, 0, 1, 1, 1), K[H] = new tt("IPv4", "DHCP IPoE", -1, 0, 1, 1, 1), 
        K[L] = new tt("IPv4", "Bridge", 1340, 6, 0, 0, 0), K[T] = new tt("IPv4", "Bridge", 34, 4, 0, 0, 0), 
        K[q] = new tt("IPv4", "Bridge", 4093, 4, 0, 0, 0), K[S] = new tt("IPv4", "Bridge", 4092, 7, 0, 0, 0), 
        e.exports.wizard_profiles = et, e.exports.regions_as_opts = function() {
            return et().map(function(t, e) {
                return {
                    text: t.name,
                    value: e
                };
            });
        }, e.exports.subregions_as_opts = function(t) {
            t = parseInt(t);
            return et()[t].subregions.map(function(t, e) {
                return {
                    text: t.name,
                    value: e
                };
            });
        }, e.exports.profiles_as_opts = function(t, e) {
            t = parseInt(t), e = parseInt(e);
            return et()[t].subregions[e].profiles.map(function(t, e) {
                return {
                    text: t.name,
                    value: e
                };
            });
        }, e.exports.getProfile = function(t, e, n) {
            t = parseInt(t), e = parseInt(e), n = parseInt(n);
            return et()[t].subregions[e].profiles[n];
        }, e.exports.getServiceDetail = function(t) {
            return K[t];
        };
    }, {} ],
    121: [ function(t, e, n) {
        "use strict";
        function i(t, e, n) {
            this._name = "PPPoE", this._login = t, this._password = e, this._options = n;
        }
        function s(t, e) {
            this._name = "TAG", this._vid = t, this._vprio = e;
        }
        function o(t) {
            this._options = t, this._name = "IPoE";
        }
        function r(t, e) {
            this._name = "Wifi", this._data = t, this._wlan_id = e;
        }
        function a(t, e) {
            this._name = e, this._service_name = e, t.lan1 && (this.lan1 = !0), t.lan2 && (this.lan2 = !0), 
            t.lan3 && (this.lan3 = !0), t.lan4 && (this.lan4 = !0);
        }
        i.prototype.apply = function() {
            var t = {
                login: this._login,
                password: this._password
            };
            return this.tag && (t.tag = this.tag.rpc_params()), cpe().multiwan.alloc_pppoe(t, this._options);
        }, s.prototype.rpc_params = function() {
            return {
                vid: this._vid,
                vprio: this._vprio
            };
        }, o.prototype.apply = function() {
            var t = {};
            return this.tag && (t.tag = this.tag.rpc_params()), cpe().multiwan.alloc_ipoe(t, this._options);
        }, r.prototype.apply = function() {
            return cpe().wifi.set(this._wlan_id, this.rpc_params());
        }, r.prototype.rpc_params = function() {
            return {
                ssid: this._data.ssid,
                pass: this._data.password,
                disable: !this._data.enabled
            };
        }, a.prototype.apply = function() {
            var t = {};
            return this.lan1 && (t.lan1 = !0), this.lan2 && (t.lan2 = !0), this.lan3 && (t.lan3 = !0), 
            this.lan4 && (t.lan4 = !0), this.tag && (t.tag = this.tag.rpc_params()), cpe().multiwan.alloc_bridge(t, {});
        }, e.exports.PPPoE = i, e.exports.TAG = s, e.exports.IPoE = o, e.exports.Wifi = r, 
        e.exports.Bridge = a;
    }, {} ],
    122: [ function(t, e, n) {
        "use strict";
        function i() {
            this._services = [];
        }
        i.prototype.wait_user_action = function() {
            var n = this;
            return new Promise(function(t, e) {
                n._resolve = t, n._reject = e;
            });
        }, i.prototype.next = function(t) {
            this._resolve && this._resolve(t);
        }, i.prototype.profile = function() {
            return this._prof;
        }, i.prototype.setProfile = function(t) {
            this._prof = t;
        }, i.prototype.add_service = function(t) {
            this._services.push(t);
        }, i.prototype.services = function() {
            return this._services;
        }, i.prototype.services_exclude = function(t) {
            this._services = this._services.filter(t);
        }, i.prototype.services_exclude_by_name = function(e) {
            this.services_exclude(function(t) {
                return t._name != e;
            });
        }, i.prototype.services_filter_by_name = function(e) {
            return this._services.filter(function(t) {
                return t._name == e;
            });
        }, e.exports.Session = i;
    }, {} ],
    123: [ function(t, e, n) {
        "use strict";
        var i = t("../flow.js").Flow, s = t("./session.js").Session, o = t("./region_list.js").getProfile, r = t("./region_list.js").getServiceDetail, a = t("./service.js"), l = a.IPoE, c = a.PPPoE, u = a.Bridge, d = a.TAG, p = a.Wifi, h = t("../../../../lib/js/nbn_lib.js").await_forEach;
        function _() {
            this._session = new s();
        }
        function f(t) {
            return {
                wifi2: {
                    enabled: !t[0].disabled,
                    ssid: t[0].ssid,
                    password: t[0].pass
                },
                wifi5: {
                    enabled: !t[1].disabled,
                    ssid: t[1].ssid,
                    password: t[1].pass
                }
            };
        }
        function m(n) {
            return T().session().services_exclude_by_name("Wifi"), cpe().wifi.all().then(f).then(function(t) {
                return e = n, t = t, T().render_and_wait_user("wifi", t).then(function(t) {
                    if (t && t.is_back) e.back(); else {
                        if (!t || !t.is_home) return T().session().add_service(new p(t.wifi2, 0)), T().session().add_service(new p(t.wifi5, 1)), 
                        e.go("confirm"), !0;
                        e.go("exit-home");
                    }
                });
                var e;
            });
        }
        function v(n) {
            return cpe().wifi.all().then(f).then(function(t) {
                var e = T().session().services_filter_by_name("Wifi"), e = {
                    wifi2: e[0]._data,
                    wifi5: e[1]._data
                }, t = {
                    is_wifi_changed: JSON.stringify(t) !== JSON.stringify(e),
                    wifi: e
                }, e = T().session().services_filter_by_name("PPPoE");
                return 0 == e.length ? (t.login = "", t.password = "") : (t.login = e[0]._login, 
                t.password = e[0]._password), T().render_and_wait_user("confirm", t).then(function(t) {
                    if (t && t.is_back) n.back(); else {
                        if (!t || !t.is_home) return n.go("apply"), !0;
                        n.go("exit-home");
                    }
                });
            });
        }
        function b(e) {
            return cpe().ports.wan.status().then(function(t) {
                return t.enabled ? void e.go("profiles") : T().render_and_wait_user("nowan").then(function(t) {
                    t && t.is_back || t && t.is_home ? e.go("exit-home") : e.go("begin");
                });
            });
        }
        function g(n) {
            return T().session().services().length = 0, T().render_and_wait_user("profiles").then(function(t) {
                if (t && t.is_back) n.go("exit-home"); else {
                    if (!t || !t.is_home) {
                        var e = o(t.reg, t.sub_reg, t.profile);
                        T().session().setProfile(e);
                        t = r(e.detail.PVC1);
                        return "PPPoE" != t.connType ? ("DHCP IPoE" == t.connType && (t = {
                            acl: e.detail.acl,
                            isDefault: !0,
                            igmp: 1 == t.igmp,
                            ipv6: "IPv4+IPv6" == t.protocol,
                            isInternet: !0
                        }, {
                            end: 0,
                            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                            BUILD: "debug"
                        }.CONFIG_LUNA || (t.isTR069 = 1 == e.detail.tr069), t = new l(t), T().session().add_service(t)), 
                        0 != e.detail.PVC2 ? n.go("tv") : 0 != e.detail.PVC3 ? n.go("voip") : n.go("wifi")) : n.go("pppoe"), 
                        !0;
                    }
                    n.go("exit-home");
                }
            });
        }
        function x(n) {
            var t = T().session().services_filter_by_name("PPPoE");
            T().session().services_exclude_by_name("PPPoE");
            var i = T().session().profile(), s = r(i.detail.PVC1), e = void 0;
            0 < t.length && (e = {
                login: t[0]._login,
                password: t[0]._password
            });
            e = {
                is_tag: -2 == s.vlanId,
                preconfig: e
            };
            return T().render_and_wait_user("PPPoE", e).then(function(t) {
                if (t && t.is_back) n.back(); else {
                    if (!t || !t.is_home) {
                        var e = {
                            acl: i.detail.acl,
                            isDefault: !0,
                            igmp: 1 == s.igmp,
                            ipv6: "IPv4+IPv6" == s.protocol,
                            isInternet: !0
                        };
                        !{
                            end: 0,
                            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                            BUILD: "debug"
                        }.CONFIG_LUNA && (e.isTR069 = 1 == i.detail.tr069);
                        e = new c(t.login, t.password, e);
                        return -2 == s.vlanId ? e.tag = new d(t.VID, t.VPRIO) : 0 < s.vlanId && (e.tag = new d(s.vlanId, s.vlanPriority)), 
                        T().session().add_service(e), 0 != i.detail.PVC2 ? n.go("tv") : 0 != i.detail.PVC3 ? n.go("voip") : n.go("wifi"), 
                        !0;
                    }
                    n.go("exit-home");
                }
            });
        }
        function w(e) {
            var t = T().session().services_filter_by_name("PPPoE"), t = {
                login: t[0]._login,
                password: t[0]._password
            };
            return T().render_and_wait_user("pppoe_fail", t).then(function(t) {
                return e.go("exit-home"), !0;
            });
        }
        function y(t) {
            return T().pppoe_fail_count || (T().pppoe_fail_count = 0), T().pppoe_fail_count++, 
            2 < T().pppoe_fail_count ? (t.go("pppoe-fail"), Promise.resolve(!0)) : T().render_and_wait_user("pppoe_again", {}).then(function(e) {
                if (e && e.is_back) t.go("exit-home"); else {
                    if (!e || !e.is_home) return cpe().multiwan.drv_default_wan().then(function(t) {
                        return cpe().multiwan.set(t.iface, e);
                    }).then(function() {
                        return t.go("apply-pppoe"), !0;
                    });
                    t.go("exit-home");
                }
            });
        }
        function j(n) {
            T().session().services_exclude_by_name("voip");
            var t = T().session().profile(), i = r(t.detail.PVC3), e = {
                lan1: !1,
                lan2: !1,
                lan3: !1,
                lan4: !1
            }, s = T().session().services().filter(function(t) {
                return "tv" == t._service_name;
            });
            0 < s.length && [ 1, 2, 3, 4 ].map(function(t) {
                return "lan" + t;
            }).forEach(function(t) {
                s[0][t] && (e[t] = !0);
            });
            t = {
                is_tag: -2 == i.vlanId,
                lans_disabled: e
            };
            return T().render_and_wait_user("voip", t).then(function(t) {
                if (t && t.is_back) n.back(); else {
                    if (!t || !t.is_home) {
                        var e = new u(t, "voip");
                        return -2 == i.vlanId ? e.tag = new d(t.VID, t.VPRIO) : 0 < i.vlanId && (e.tag = new d(i.vlanId, i.vlanPriority)), 
                        T().session().add_service(e), n.go("wifi"), !0;
                    }
                    n.go("exit-home");
                }
            });
        }
        function k(n) {
            T().session().services_exclude_by_name("tv");
            var i = T().session().profile(), s = r(i.detail.PVC2), t = {
                is_tag: -2 == s.vlanId
            };
            return T().render_and_wait_user("tv", t).then(function(t) {
                if (t && t.is_back) n.back(); else {
                    if (!t || !t.is_home) {
                        var e = new u(t, "tv");
                        return -2 == s.vlanId ? e.tag = new d(t.VID, t.VPRIO) : 0 < s.vlanId && (e.tag = new d(s.vlanId, s.vlanPriority)), 
                        T().session().add_service(e), 0 != i.detail.PVC3 ? n.go("voip") : n.go("wifi"), 
                        !0;
                    }
                    n.go("exit-home");
                }
            });
        }
        function N() {
            var t = T().session().services();
            return h(t, function(t) {
                return t.apply();
            });
        }
        function I(t, e) {
            this.flow = t, this.count = e;
        }
        function E(t) {
            return T().session().services().find(function(t) {
                return "PPPoE" == t._name;
            }) ? new I(t, 6).start() : (t.go("done"), !0);
        }
        function P(t) {
            var e, n, i, s, o;
            return e = T().session().profile(), "DHCP IPoE" == (i = r(e.detail.PVC4)).connType && (i = {
                isTR069: 4 == e.detail.tr069,
                igmp: 1 == i.igmp,
                ipv6: "IPv4+IPv6" == i.protocol
            }, i = new l(i), T().session().add_service(i)), i = T().session().services(), s = i.find(function(t) {
                return "tv" == t._name;
            }), o = i.find(function(t) {
                return "voip" == t._name;
            }), s && o && !s.tag && !o.tag && (n = {}, [ 1, 2, 3, 4 ].map(function(t) {
                return "lan" + t;
            }).forEach(function(t) {
                (s[t] || o[t]) && (n[t] = !0);
            }), T().session().services_exclude_by_name("tv"), T().session().services_exclude_by_name("voip"), 
            i = new u(n, "untag_bridge"), T().session().add_service(i)), T().render_applying("applying").then(function() {
                return cpe().multiwan.free_all();
            }).then(N).then(function() {
                return cpe().apply();
            }).then(function() {
                return E(t);
            });
        }
        function A(t) {
            return T().render_applying("applying").then(function() {
                return cpe().apply();
            }).then(function() {
                return E(t);
            });
        }
        function C(t) {
            return Promise.resolve(!0).then(function(t) {
                return T()._config.go_home(), !0;
            });
        }
        function D(t) {
            return T().render_and_wait_user("done").then(function(t) {
                return T()._config.success(), !0;
            });
        }
        _.prototype.session = function() {
            return this._session;
        }, _.prototype.render_applying = function(t, e) {
            e = this._RM.render_component(t, e);
            return this._RM.remount(e, this._id), this._current_page = e, new Promise(function(t, e) {
                t();
            });
        }, _.prototype.render_page = function(t, e, n, i) {
            i = n.render_component(e, i);
            n.remount(i, t), this._current_page = i;
            var s = this;
            return n.get_component(i).exports.on("next", function(t) {
                s.session().next(t);
            }), n.get_component(i).exports.on("back", function(t) {
                s.session().next({
                    is_back: !0
                });
            }), n.get_component(i).exports.on("exit-home", function(t) {
                s.session().next({
                    is_home: !0
                });
            }), this;
        }, _.prototype.render_and_wait_user = function(t, e) {
            return this.render_page(this._id, t, this._RM, e).session().wait_user_action();
        }, _.prototype.start_new_sesion = function(t, e, n) {
            this._session = new s(), this._id = t, this._RM = e, this._config = n;
        }, I.prototype.start = function() {
            var i = this;
            return new Promise(function(n, t) {
                setTimeout(function e() {
                    cpe().multiwan.drv_default_wan().then(function(t) {
                        return i.count--, "S_CONNECTED" == t.status_text ? (i.flow.go("done"), n(!0)) : "S_CONNECTING" != t.status_text || i.count <= 0 ? (i.flow.go("pppoe-again"), 
                        n(!0)) : void setTimeout(e, 3e3);
                    });
                }, 1e3);
            });
        }, _.prototype.run = function(t, e, n) {
            return n && n.success && n.go_home || console.error('Wizard: the 3 arg(config) is not found or not full\n\nExample:\n    var config = {\n        go_home: function(){\n\t        window.location.href ="status.html"\n        },\n        success: function(){\n\t        window.location.href ="http://VENDOR.ru"\n        }\n    };\n\n    wizard().run("app", RM, config);\nOR:\n\twizard_package().run("app",config);'), 
            this.start_new_sesion(t, e, n), app && app().wizard && (app().wizard.current = T), 
            this._flow = new i(), this._flow.stage("begin", b).stage("profiles", g).stage("pppoe", x).stage("tv", k).stage("voip", j).stage("wifi", m).stage("confirm", v).stage("apply", P).stage("apply-pppoe", A).stage("pppoe-again", y).stage("pppoe-fail", w).stage("exit-home", C).stage("done", D).start_async();
        };
        var L = {};
        function T() {
            return L._wizard || (L._wizard = new _()), L._wizard;
        }
        e.exports.wizard = T;
    }, {
        "../../../../lib/js/nbn_lib.js": 18,
        "../flow.js": 119,
        "./region_list.js": 120,
        "./service.js": 121,
        "./session.js": 122
    } ],
    124: [ function(b, t, e) {
        "use strict";
        var g = b("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n, i = b("multilang.js").lang, s = b("event-emitter.js").EventEmiter, o = i(), r = {}, a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, i = {}, e = e.attr, h = e.preconfig, _ = e.is_tag || !1, f = new s();
            (this.exports = {}).on = function(t, e) {
                return f.on(t, e);
            }, h && (n = h.tag);
            var m = [ c, u ];
            function v() {
                m.filter(function(t) {
                    return 0 == t.el.e.value.length;
                });
                var t = m.filter(function(t) {
                    return /[^a-zA-Z0-9_\.\\/\!@#$&*-]/.test(t.el.e.value);
                });
                d.exports.is_valid() && 0 == t.length ? f.emit("form-valid") : f.emit("form-invalid");
            }
            this.obj = {
                created: function() {
                    h && (c.el.e.value = h.login, u.el.e.value = h.password), r.el.disabled(!1), m.forEach(function(t) {
                        return t.el.on("input", v);
                    }), p.el.on("click", function(t) {
                        return e = u, n = p, t.preventDefault(), e = e.el.e, n = n.el.e, void ("password" === e.type ? (e.type = "text", 
                        n.classList.add("show")) : (e.type = "password", n.classList.remove("show")));
                        var e, n;
                    });
                },
                mounted: function() {
                    a.el.on("click", function() {
                        return f.emit("back");
                    }), l.el.on("click", function() {
                        return f.emit("exit-home");
                    }), r.el.on("click", function(t) {
                        _ ? f.emit("next", {
                            login: c.el.e.value,
                            password: u.el.e.value,
                            VID: d.exports.get_vid(),
                            VPRIO: d.exports.get_vprio()
                        }) : f.emit("next", {
                            login: c.el.e.value,
                            password: u.el.e.value
                        });
                    }), f.on("form-valid", function(t) {
                        return r.el.disabled(!1);
                    }), f.on("form-invalid", function(t) {
                        return r.el.disabled(!0);
                    }), _ && (d.exports.on("form-invalid", function(t) {
                        return r.el.disabled(!0);
                    }), d.exports.on("form-valid", v)), v(), f.emit("mounted", this);
                }
            }, this.tree = new g("div", {}), this.tree.root().set_class("pppoe_container container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("pppoe_wrapper wrapper").text("\n            ").child("div", {}).set_class("pppoe_desc desc").text("\n                ").child("p", {
                text: o.wizard.pppoe_description
            }).up().text("\n                ").child("p", {
                text: o.wizard.vlan_description
            }).up().text("\n            ").up().text("\n            ").child("div", {}).set_class("alert-message").text("\n                ").child("p", {}).set_class("warning").bind(i).directive("bind", i).up().text("\n              ").up().text("\n            ").child("div", {}).set_class("pppoe_controls").text("\n                ").child("ul", {}).set_class("pppoe_controls-list").text("\n                    ").child("li", {}).set_class("pppoe_list-item").text("\n                        ").child("label", {
                text: o.wizard.login_text
            }).set_class("label_wizard pppoe_list-text").up().text("\n                        ").child("input", {
                name: "login",
                id: "login",
                type: "text"
            }).set_class("input").bind(c).directive("bind", c).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("pppoe_list-item").text("\n                        ").child("label", {
                text: o.wizard.pass_text
            }).set_class("label_wizard pppoe_list-text").up().text("\n                        ").child("input", {
                maxlength: "64",
                name: "password",
                id: "password",
                type: "password"
            }).set_class("input").bind(u).directive("bind", u).up().text("\n                        ").child("a", {
                id: "eye"
            }).set_class("btn_eye").bind(p).directive("bind", p).up().text("\n                    ").up().text("\n                ").up().text("\n            ").up().text("\n            ").child("vlan-tag", {
                is_tag: _,
                preconfig: n
            }).bind(d).directive("bind", d).up().text("\n            ").child("div", {}).set_class("nav_buttons buttons_container").text("\n                ").child("input", {
                value: o.button.back,
                id: "back",
                type: "button"
            }).set_class("link_bg-btn").bind(a).directive("bind", a).up().text("\n                ").child("input", {
                value: o.button.manual,
                id: "manual",
                type: "button"
            }).set_class("link_bg-btn").bind(l).directive("bind", l).up().text("\n                ").child("input", {
                value: o.button.next,
                id: "next",
                type: "button"
            }).set_class("link_bg-btn").bind(r).directive("bind", r).up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    125: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("multilang.js").lang, n = (r("event-emitter.js").EventEmiter, n());
            n.login = "login", n.password = "password", n.next = "next", n.back = "back", n.save = "save file";
            var i = {}, s = e.attr.time || 6;
            function o() {
                0 < s && (s--, i.el.set(s), setTimeout(o, 1e3));
            }
            this.obj = {
                mounted: function() {
                    o();
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("app_container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("app_wrapper").text("\n            ").child("div", {}).set_class("app_desc desc").text("\n                ").child("p", {
                text: n.wizard.apply_descr
            }).set_class("app_desc-text").up().text("\n                ").child("p", {}).set_class("app_desc-timer").bind(i).directive("bind", i).up().text("\n            ").up().text("\n            ").child("div", {}).set_class("app_warning").text("\n                ").child("p", {
                text: n.wizard.apply_warning
            }).set_class("app_warning-text").up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    126: [ function(m, t, e) {
        "use strict";
        var v = m("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = m("multilang.js").lang, i = m("event-emitter.js").EventEmiter, n = n(), s = {}, o = {}, r = {}, a = {}, l = {}, c = {}, u = {}, d = {}, e = e.attr, p = e.is_tag || !1, h = e.lans_max || 4, _ = e.lans_disabled || {
                lan1: !1,
                lan2: !1,
                lan3: !1,
                lan4: !1
            }, f = (e.title, new i());
            (this.exports = {}).on = function(t, e) {
                return f.on(t, e);
            }, this.obj = {
                created: function() {
                    a.el.disabled(_.lan1), a.el.e.checked = _.lan1, l.el.disabled(_.lan2), l.el.e.checked = _.lan2, 
                    c.el.disabled(_.lan3), c.el.e.checked = _.lan3, u.el.disabled(_.lan4), u.el.e.checked = _.lan4, 
                    l.el.show(1 < h), c.el.show(2 < h), u.el.show(3 < h), p && o.el.disabled(!0);
                },
                mounted: function() {
                    s.el.on("click", function() {
                        return f.emit("back");
                    }), r.el.on("click", function() {
                        return f.emit("exit-home");
                    }), o.el.on("click", function(t) {
                        var e = {
                            lan1: !_.lan1 && a.el.e.checked
                        };
                        1 < h && (e.lan2 = !_.lan2 && l.el.e.checked), 2 < h && (e.lan3 = !_.lan3 && c.el.e.checked), 
                        3 < h && (e.lan4 = !_.lan4 && u.el.e.checked), p && (e.VID = d.exports.get_vid(), 
                        e.VPRIO = d.exports.get_vprio()), f.emit("next", e);
                    }), p && (d.exports.on("form-invalid", function(t) {
                        return o.el.disabled(!0);
                    }), d.exports.on("form-valid", function(t) {
                        return o.el.disabled(!1);
                    })), f.emit("mounted", this);
                }
            }, this.tree = new v("div", {}), this.tree.root().set_class("bridge_container").text("\n    ").child("div", {}).set_class("bridge_wrapper").text("\n      ").child("div", {}).set_class("bridge_lan").text("\n        ").child("ul", {}).set_class("bridge_lan-list").text("\n          ").child("li", {}).set_class("lan_list-item").text("\n            ").child("label", {
                text: n.wizard.lan1,
                for: ""
            }).set_class("label lan_list-text").up().text("\n            ").child("checkbox", {
                id: "lan1"
            }).bind(a).directive("bind", a).up().text("\n          ").up().text("\n          ").child("li", {}).set_class("lan_list-item").text("\n            ").child("label", {
                text: n.wizard.lan2,
                for: ""
            }).set_class("label lan_list-text").up().text("\n            ").child("checkbox", {
                id: "lan2"
            }).bind(l).directive("bind", l).up().text("\n          ").up().text("\n          ").child("li", {}).set_class("lan_list-item").text("\n            ").child("label", {
                text: n.wizard.lan3,
                for: ""
            }).set_class("label lan_list-text").up().text("\n            ").child("checkbox", {
                id: "lan3"
            }).bind(c).directive("bind", c).up().text("\n          ").up().text("\n          ").child("li", {}).set_class("lan_list-item").text("\n            ").child("label", {
                text: n.wizard.lan4,
                for: ""
            }).set_class("label lan_list-text").up().text("\n            ").child("checkbox", {
                id: "lan4"
            }).bind(u).directive("bind", u).up().text("\n          ").up().text("\n        ").up().text("\n      ").up().text("\n      ").child("vlan-tag", {
                is_tag: p
            }).bind(d).directive("bind", d).up().text("\n\n      ").child("div", {}).set_class("nav_buttons buttons_container").text("\n        ").child("input", {
                value: n.button.back,
                id: "back",
                type: "button"
            }).set_class("link_bg-btn").bind(s).directive("bind", s).up().text("\n        ").child("input", {
                value: n.button.manual,
                id: "manual",
                type: "button"
            }).set_class("link_bg-btn").bind(r).directive("bind", r).up().text("\n        ").child("input", {
                value: n.button.next,
                id: "next",
                type: "button"
            }).set_class("link_bg-btn").bind(o).directive("bind", o).up().text("\n      ").up().text("\n    ").up().text("\n  ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    127: [ function(h, t, e) {
        "use strict";
        var _ = h("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = h("multilang.js").lang, i = h("event-emitter.js").EventEmiter, s = n(), o = (e.attr.mib, 
            {}), r = {}, a = {}, l = {}, c = {}, u = e.attr.login || "", n = e.attr.password || "", d = e.attr.is_wifi_changed || !1, e = e.attr.wifi || {
                wifi2: {
                    enabled: !0,
                    ssid: "unknown",
                    password: "unknown"
                },
                wifi5: {
                    enabled: !0,
                    ssid: "unknown",
                    password: "unknown"
                }
            }, p = new i();
            (this.exports = {}).on = function(t, e) {
                return p.on(t, e);
            }, this.obj = {
                created: function() {
                    a.el.show(!1), l.el.show(!1), c.el.show(d);
                },
                mounted: function() {
                    o.el.on("click", function() {
                        return p.emit("back");
                    }), r.el.on("click", function(t) {
                        p.emit("next", {});
                    }), p.emit("mounted", this);
                }
            }, this.tree = new _("div", {}), this.tree.root().set_class("wifi_container container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("wifi_wrapper wrapper").text("\n            ").child("div", {}).set_class("wifi_desc desc").text("\n                ").child("p", {
                text: s.wizard.message
            }).set_class("wifi_desc-text warning-text").up().text("\n                ").child("p", {
                text: s.wizard.info
            }).set_class("wifi_desc-text").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("confirm_desc").text("\n                ").child("p", {
                text: s.warning.wifi_is_changed
            }).set_class("wifi_desc-text warning").bind(c).directive("bind", c).up().text("\n            ").up().text("\n            ").child("div", {}).set_class("confirm_controls").text("\n                ").child("ul", {}).set_class("confirm_wifi-list").text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.wifi2
            }).set_class("label_header").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.name
            }).set_class("label_confirm").up().text("\n                        ").child("label", {
                text: e.wifi2.ssid,
                id: "ssid2",
                type: "text"
            }).set_class('""').up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.pass,
                for: "wifiPass2"
            }).set_class("label_confirm").up().text("\n                        ").child("label", {
                text: e.wifi2.password,
                id: "password2",
                type: "password"
            }).set_class('""').up().text("\n                    ").up().text("\n                ").up().text("\n                ").child("ul", {}).set_class("confirm_wifi-list").text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.wifi5
            }).set_class("label_header").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.name,
                for: "wifiName5"
            }).set_class("label_confirm").up().text("\n                        ").child("label", {
                text: e.wifi5.ssid,
                id: "ssid5",
                type: "text"
            }).set_class('""').up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.pass,
                for: "wifiPass5"
            }).set_class("label_confirm").up().text("\n                        ").child("label", {
                text: e.wifi5.password,
                id: "password5",
                type: "password"
            }).set_class('""').up().text("\n                    ").up().text("\n                ").up().text("\n                ").child("ul", {}).set_class("confirm_info-list").bind(a).directive("bind", a).text("\n                    ").child("li", {}).set_class("confirm_list-item").text("\n                        ").child("label", {
                text: s.wizard.ip,
                for: ""
            }).set_class("label_confirm confirm_item-text").up().text("\n                        ").child("label", {
                mib: "lan_ip",
                id: ""
            }).set_class('""').up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("confirm_list-item").text("\n                        ").child("label", {
                text: s.wizard.login_rule,
                for: ""
            }).set_class("label_confirm confirm_item-text").up().text("\n                        ").child("label", {
                text: u,
                id: "login"
            }).set_class("label_confirm confirm_item-text").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("confirm_list-item").text("\n                        ").child("label", {
                text: s.wizard.pass_rule
            }).set_class("label_confirm confirm_item-text").up().text("\n                        ").child("label", {
                text: n,
                id: "password"
            }).set_class("label_confirm confirm_item-text").up().text("\n                    ").up().text("\n                ").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("nav_buttons buttons_container").text("\n                ").child("input", {
                value: s.button.back,
                id: "back",
                type: "button"
            }).set_class("link_bg-btn").bind(o).directive("bind", o).up().text("\n                ").child("input", {
                value: s.button.save,
                id: "save",
                type: "button"
            }).set_class("link_bg-btn").bind(l).directive("bind", l).up().text("\n                ").child("input", {
                value: s.button.next,
                id: "next",
                type: "button"
            }).set_class("link_bg-btn").bind(r).directive("bind", r).up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    128: [ function(r, t, e) {
        "use strict";
        var a = r("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = r("multilang.js").lang, i = r("event-emitter.js").EventEmiter, n = n();
            n.login = "login", n.password = "password", n.next = "next", n.back = "back", n.save = "save file";
            var s = {}, o = new i();
            (this.exports = {}).on = function(t, e) {
                return o.on(t, e);
            }, this.obj = {
                created: function() {},
                mounted: function() {
                    s.el.on("click", function(t) {
                        o.emit("next", {});
                    }), o.emit("mounted", this);
                }
            }, this.tree = new a("div", {}), this.tree.root().set_class("done_container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("done_wrapper wrapper").text("\n            ").child("div", {}).set_class("done_desc desc").text("\n                ").child("p", {
                text: n.wizard.done
            }).set_class("done_desc-text").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("done_btn buttons_container").text("\n                ").child("input", {
                value: n.wizard.link,
                type: "button",
                id: "next"
            }).set_class("link_bg-btn done_button").bind(s).directive("bind", s).up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    129: [ function(l, t, e) {
        "use strict";
        var c = l("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = l("multilang.js").lang, i = l("event-emitter.js").EventEmiter, n = n(), s = {}, o = {}, r = {}, a = new i();
            (this.exports = {}).on = function(t, e) {
                return a.on(t, e);
            }, this.obj = {
                created: function() {
                    s.el.on("click", function() {
                        return a.emit("back");
                    }), o.el.on("click", function() {
                        return a.emit("next");
                    }), r.el.on("click", function() {
                        return a.emit("exit-home");
                    });
                }
            }, this.tree = new c("div", {}), this.tree.root().set_class("nowan_container container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("nowan_wrapper wrapper").text("\n            ").child("div", {}).set_class("nowan_desc desc").text("\n                ").child("p", {
                text: n.wizard.nowan_warning
            }).set_class("nowan_desc-text warning").up().text("\n                ").child("p", {
                text: n.wizard.nowan_desc
            }).set_class("nowan_desc-text").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("nowan_img").text("\n                ").child("img", {
                id: "wanPicture",
                src: "WAN.gif",
                border: "0"
            }).up().text("\n            ").up().text(">\n            ").child("div", {}).set_class("nowan_info").text("\n                ").child("ul", {}).set_class("nowan_info-list").text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.model,
                mib: "model_name"
            }).set_class("label nowan_item-text").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.ver,
                mib: "hw_version"
            }).set_class("label nowan_item-text").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.sw_ver,
                mib: "fw_version"
            }).set_class("label nowan_item-text").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.mac,
                mib: "mac_address"
            }).set_class("label nowan_item-text").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.sn,
                mib: "serial_number"
            }).set_class("label nowan_item-text").up().text("\n                    ").up().text("\n                ").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("nav_buttons buttons_container").text("\n                ").child("input", {
                value: n.button.back,
                id: "back",
                type: "button"
            }).set_class("link_bg-btn").bind(s).directive("bind", s).up().text("\n                ").child("input", {
                value: n.button.manual,
                id: "manual",
                type: "button"
            }).set_class("link_bg-btn").bind(r).directive("bind", r).up().text("\n                ").child("input", {
                value: n.button.next,
                id: "next",
                type: "button"
            }).set_class("link_bg-btn").bind(o).directive("bind", o).up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    130: [ function(u, t, e) {
        "use strict";
        var d = u("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = u("multilang.js").lang, i = u("event-emitter.js").EventEmiter, n = n(), s = {}, o = {}, r = {}, a = {}, l = {}, c = new i();
            (this.exports = {}).on = function(t, e) {
                return c.on(t, e);
            }, this.obj = {
                created: function() {},
                mounted: function() {
                    a.el.on("click", function() {
                        return c.emit("back");
                    }), l.el.on("click", function() {
                        return c.emit("exit-home");
                    }), r.el.on("click", function(t) {
                        c.emit("next", {
                            login: s.el.e.value,
                            password: o.el.e.value
                        });
                    }), c.emit("mounted", this);
                }
            }, this.tree = new d("div", {}), this.tree.root().set_class("fail_container container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("fail_wrapper wrapper").text("\n            ").child("div", {}).set_class("fail_desc desc").text("\n                ").child("p", {
                text: n.wizard.descr_fail,
                id: "descr"
            }).set_class("fail_desc-text").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("fail_controls").text("\n                ").child("ul", {}).set_class("fail_controls-list").text("\n                    ").child("li", {}).set_class("fail_list-item").text("\n                        ").child("label", {
                text: n.wizard.login_text,
                for: ""
            }).set_class("label_ppoe fail_list-text").up().text("\n                        ").child("input", {
                id: "",
                type: "text"
            }).set_class("fail_list-login input").bind(s).directive("bind", s).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("fail_list-item").text("\n                        ").child("label", {
                text: n.wizard.pass_text,
                for: ""
            }).set_class("fail_list-text label_ppoe").up().text("\n                        ").child("input", {
                id: "",
                type: "password"
            }).set_class("fail_list-pass input").bind(o).directive("bind", o).up().text("\n                    ").up().text("\n                ").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("nav_buttons buttons_container").text("\n                ").child("input", {
                value: n.button.back,
                id: "manual",
                type: "button"
            }).set_class("link_bg-btn").bind(a).directive("bind", a).up().text("\n                ").child("input", {
                value: n.button.manual,
                id: "manual",
                type: "button"
            }).set_class("link_bg-btn").bind(l).directive("bind", l).up().text("\n                ").child("input", {
                value: n.button.next,
                id: "next",
                type: "button"
            }).set_class("link_bg-btn").bind(r).directive("bind", r).up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    131: [ function(l, t, e) {
        "use strict";
        var c = l("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = l("multilang.js").lang, i = l("event-emitter.js").EventEmiter, n = n(), s = {}, o = {}, r = {}, a = new i();
            (this.exports = {}).on = function(t, e) {
                return a.on(t, e);
            };
            i = e.attr.login || "", e = e.attr.password || "";
            this.obj = {
                created: function() {
                    s.el.on("click", function() {
                        return a.emit("back");
                    }), o.el.on("click", function() {
                        return a.emit("next");
                    }), r.el.on("click", function() {
                        return a.emit("exit-home");
                    });
                }
            }, this.tree = new c("div", {}), this.tree.root().set_class("nowan_container container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("nowan_wrapper wrapper").text("\n            ").child("div", {}).set_class("nowan_desc desc").text("\n                ").child("p", {
                text: n.wizard.pppoe_fail_warning
            }).set_class("nowan_desc-text warning").up().text("\n                ").child("p", {
                text: n.wizard.pppoe_fail_desc
            }).set_class("nowan_desc-text").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("nowan_info").text("\n                ").child("ul", {}).set_class("nowan_info-list").text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("label", {
                text: n.wizard.pppoe_name
            }).set_class("nowan_item-text").up().text("\n                        ").child("label", {
                text: i,
                id: "login"
            }).set_class("nowan_item-text").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("label", {
                text: n.wizard.pppoe_pass
            }).set_class("nowan_item-text").up().text("\n                        ").child("label", {
                text: e,
                id: "password"
            }).set_class("nowan_item-text").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("label", {
                text: n.wizard.ser_info
            }).set_class("nowan_item-text").up().text("\n                        ").child("label", {
                id: ""
            }).set_class("nowan_item-text").up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.model,
                mib: "model_name"
            }).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.ver,
                mib: "hw_version"
            }).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.sw_ver,
                mib: "fw_version"
            }).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.mac,
                mib: "mac_address"
            }).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("nowan_list-item").text("\n                        ").child("no-login-static-text", {
                text: n.wizard.sn,
                mib: "serial_number"
            }).up().text("\n                    ").up().text("\n                ").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("nav_buttons buttons_container").text("\n                ").child("input", {
                value: n.button.back,
                id: "back",
                type: "button"
            }).set_class("link_bg-btn").bind(s).directive("bind", s).up().text("\n                ").child("input", {
                value: n.button.manual,
                id: "manual",
                type: "button"
            }).set_class("link_bg-btn").bind(r).directive("bind", r).up().text("\n                ").child("input", {
                value: n.button.next,
                id: "next",
                type: "button"
            }).set_class("link_bg-btn").bind(o).directive("bind", o).up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    132: [ function(m, t, e) {
        "use strict";
        var v = m("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = m("multilang.js").lang, i = m("event-emitter.js").EventEmiter, s = m("region_list.js").regions_as_opts, o = m("region_list.js").subregions_as_opts, r = m("region_list.js").profiles_as_opts, n = n(), a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = new i();
            function _(t) {
                t = t.el.e;
                return t.options[t.selectedIndex].value;
            }
            function f() {
                var t = _(u);
                d.el.e.length = 0, d.el.addOptions(o(t));
                var e = _(d);
                p.el.e.length = 0, p.el.addOptions(r(t, e));
            }
            (this.exports = {}).on = function(t, e) {
                return h.on(t, e);
            }, this.obj = {
                created: function() {
                    a.el.on("click", function() {
                        return h.emit("back");
                    }), c.el.on("click", function() {
                        return h.emit("exit-home");
                    }), u.el.addOptions(s()), f();
                },
                mounted: function() {
                    u.el.on("change", function(t) {
                        f();
                    }), d.el.on("change", function(t) {
                        var e = _(u), n = _(d);
                        p.el.e.length = 0, p.el.addOptions(r(e, n));
                    }), l.el.on("click", function(t) {
                        h.emit("next", {
                            reg: parseInt(_(u)),
                            sub_reg: parseInt(_(d)),
                            profile: parseInt(_(p))
                        });
                    }), h.emit("mounted", this);
                }
            }, this.tree = new v("div", {}), this.tree.root().set_class("profiles_container container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("profiles_wrapper wrapper").text("\n            ").child("div", {}).set_class("profiles_desc desc").text("\n                ").child("p", {
                text: n.profiles.descr
            }).set_class("profiles_desc-text").up().text("\n            ").up().text("\n\n            ").child("div", {}).set_class("profiles_controls").text("\n                ").child("ul", {}).set_class("profiles_controls-list").text("\n                    ").child("li", {}).set_class("profiles_list-item").text("\n                        ").child("label", {
                text: n.profiles.region
            }).set_class("label_profiles profiles_list-text").up().text("\n                        ").child("select", {
                id: "region"
            }).set_class("select").bind(u).directive("bind", u).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("profiles_list-item").text("\n                        ").child("label", {
                text: n.profiles.subregion
            }).set_class("label_profiles profiles_list-text").up().text("\n                        ").child("select", {
                id: "subregion"
            }).set_class("select").bind(d).directive("bind", d).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("profiles_list-item").text("\n                        ").child("label", {
                text: n.profiles.profile
            }).set_class("label_profiles profiles_list-text").up().text("\n                        ").child("select", {
                id: "profile"
            }).set_class("select").bind(p).directive("bind", p).up().text("\n                    ").up().text("\n                ").up().text("\n            ").up().text("\n\n            ").child("div", {}).set_class("nav_buttons buttons_container").text("\n                ").child("input", {
                value: n.button.back,
                id: "back",
                type: "button"
            }).set_class("link_bg-btn").bind(a).directive("bind", a).up().text("\n                ").child("input", {
                value: n.button.manual,
                id: "manual",
                type: "button"
            }).set_class("link_bg-btn").bind(c).directive("bind", c).up().text("\n                ").child("input", {
                value: n.button.next,
                id: "next",
                type: "button"
            }).set_class("link_bg-btn").bind(l).directive("bind", l).up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "region_list.js": 120,
        "virtual-dom.js": 26
    } ],
    133: [ function(_, t, e) {
        "use strict";
        var f = _("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = _("multilang.js").lang, i = _("event-emitter.js").EventEmiter, n = n(), e = e.attr, s = e.is_tag || !1, o = e.preconfig, r = {}, a = {}, l = {}, c = new i(), u = this.exports = {};
            u.on = function(t, e) {
                return c.on(t, e);
            };
            var d = [ a, l ], p = !1;
            function h() {
                0 == d.filter(function(t) {
                    return 0 == t.el.e.value.length;
                }).length ? (p = !0, console.log("form_valid "), c.emit("form-valid")) : (p = !1, 
                c.emit("form-invalid"));
            }
            this.obj = {
                created: function() {
                    o && (a.el.e.value = o.VID, l.el.e.value = o.VPRIO), r.el.show(s), d.forEach(function(t) {
                        return t.el.on("input", h);
                    });
                },
                mounted: function() {
                    u.is_valid = function() {
                        return !s || p;
                    }, u.get_vid = function() {
                        return parseInt(a.el.e.value);
                    }, u.get_vprio = function() {
                        return parseInt(l.el.e.value);
                    }, h(), c.emit("mounted", this);
                }
            }, this.tree = new f("div", {
                id: "TAG"
            }), this.tree.root().set_class("tag_controls").bind(r).directive("bind", r).text("\n        ").child("ul", {}).set_class("tag_list").text("\n            ").child("li", {}).set_class("tag_list-item").text("\n                ").child("label", {
                text: n.wizard.VLAN,
                for: ""
            }).set_class("label_wizard tag_item-text").up().text("\n                ").child("input", {
                id: "VID",
                type: "text"
            }).set_class("input").bind(a).directive("bind", a).up().text("\n            ").up().text("\n            ").child("li", {}).set_class("tag_list-item").text("\n                ").child("label", {
                text: n.wizard.priority_VLAN,
                for: ""
            }).set_class("label_wizard tag_item-text").up().text("\n                ").child("input", {
                id: "VPRIO",
                type: "text"
            }).set_class("input").bind(l).directive("bind", l).up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    134: [ function(a, t, e) {
        "use strict";
        var l = a("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = a("multilang.js").lang, i = (a("event-emitter.js").EventEmiter, n()), s = {}, o = e.attr, n = o.is_tag || !1, e = o.lans_disabled, o = o.lans_max, r = this.exports = {};
            this.obj = {
                created: function() {
                    r = Object.assign(r, s.exports);
                }
            }, this.tree = new l("div", {}), this.tree.root().set_class("tv_container container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("tv_wrapper").text("\n            ").child("div", {}).set_class("tv_desc desc").text("\n                ").child("p", {
                text: i.wizard.tv_desc
            }).set_class("tv_desc-text").up().text("\n            ").up().text("\n            ").child("bridge", {
                is_tag: n,
                lans_disabled: e,
                lans_max: o
            }).bind(s).directive("bind", s).up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    135: [ function(a, t, e) {
        "use strict";
        var l = a("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = a("multilang.js").lang, i = (a("event-emitter.js").EventEmiter, n()), s = {}, o = e.attr, n = o.is_tag || !1, e = o.lans_disabled, o = o.lans_max, r = this.exports = {};
            this.obj = {
                created: function() {
                    r = Object.assign(r, s.exports);
                }
            }, this.tree = new l("div", {}), this.tree.root().set_class("voip_container container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("voip_wrapper").text("\n        ").child("div", {}).set_class("voip_desc desc").text("\n            ").child("p", {
                text: i.wizard.voip_desc
            }).up().text("\n        ").up().text("\n        ").child("bridge", {
                is_tag: n,
                lans_disabled: e,
                lans_max: o
            }).bind(s).directive("bind", s).up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    136: [ function(g, t, e) {
        "use strict";
        var x = g("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = g("multilang.js").lang, i = g("event-emitter.js").EventEmiter, s = n(), o = {}, r = {}, a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = {}, _ = {}, f = [ u, h, d, _ ], m = new i();
            (this.exports = {}).on = function(t, e) {
                return m.on(t, e);
            };
            var v = e.attr.wifi2 || {
                enabled: !0,
                ssid: "",
                password: ""
            }, b = e.attr.wifi5 || {
                enabled: !0,
                ssid: "",
                password: ""
            };
            this.obj = {
                created: function() {
                    c.el.e.checked = v.enabled, u.el.e.value = v.ssid, d.el.e.value = v.password, p.el.e.checked = b.enabled, 
                    h.el.e.value = b.ssid, _.el.e.value = b.password, l.el.show(!1);
                    function e() {
                        var t = f.filter(function(t) {
                            return 0 == t.el.e.value.length;
                        }), e = f.filter(function(t) {
                            return /\s/.test(t.el.e.value);
                        }), n = [ d, _ ].filter(function(t) {
                            return t.el.e.value.length < 8;
                        }), i = f.filter(function(t) {
                            return /[^a-zA-Z0-9_\.\\/\!@#$&*-]/.test(t.el.e.value);
                        });
                        0 !== t.length ? (m.emit("form-invalid"), l.el.show(!0), l.el.set(s.error.empty_inputs)) : 0 !== e.length ? (m.emit("form-invalid"), 
                        l.el.show(!0), l.el.set(s.error.space_inputs)) : 0 !== n.length ? (m.emit("form-invalid"), 
                        l.el.show(!0), l.el.set(s.error.short_pass)) : 0 !== i.length ? (m.emit("form-invalid"), 
                        l.el.show(!0), l.el.set(s.error.lang_symb)) : (l.el.show(!1), m.emit("form-valid"));
                    }
                    f.forEach(function(t) {
                        return t.el.on("input", e);
                    });
                },
                mounted: function() {
                    o.el.on("click", function() {
                        return m.emit("back");
                    }), a.el.on("click", function() {
                        return m.emit("exit-home");
                    }), m.on("form-valid", function(t) {
                        return r.el.disabled(!1);
                    }), m.on("form-invalid", function(t) {
                        return r.el.disabled(!0);
                    }), r.el.on("click", function(t) {
                        m.emit("next", {
                            wifi2: {
                                enabled: c.el.e.checked,
                                ssid: u.el.e.value,
                                password: d.el.e.value
                            },
                            wifi5: {
                                enabled: p.el.e.checked,
                                ssid: h.el.e.value,
                                password: _.el.e.value
                            }
                        });
                    }), m.emit("mounted", this);
                }
            }, this.tree = new x("div", {}), this.tree.root().set_class("wifi_container container").text("\n        ").child("div", {}).set_class("logo").text("\n            ").child("img", {
                id: "logo",
                src: "rtk_logo.png",
                border: "0"
            }).up().text("\n        ").up().text("\n        ").child("div", {}).set_class("wifi_wrapper wrapper").text("\n            ").child("div", {}).set_class("wifi_desc").text("\n                ").child("p", {
                text: s.wizard.descr,
                id: "descr"
            }).set_class("wifi_desc-text").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("alert-message").text("\n              ").child("p", {}).set_class("warning").bind(l).directive("bind", l).up().text("\n            ").up().text("\n            ").child("div", {}).set_class("wifi_controls").text("\n                ").child("ul", {}).set_class("wifi_2-list").text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.enable2,
                for: "enable2"
            }).set_class("label_header").up().text("\n                        ").child("input", {
                type: "checkbox",
                id: "enable2"
            }).bind(c).directive("bind", c).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.name,
                for: "wifiName2"
            }).set_class("label_wizard").up().text("\n                        ").child("input", {
                maxlength: "16",
                id: "wifiName2",
                type: "text"
            }).set_class("input").bind(u).directive("bind", u).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.pass,
                for: "wifiPass2"
            }).set_class("label_wizard").up().text("\n                        ").child("input", {
                maxlength: "32",
                id: "wifiPass2",
                type: "password"
            }).set_class("input").bind(d).directive("bind", d).up().text("\n                    ").up().text("\n                ").up().text("\n                ").child("ul", {}).set_class("wifi_5-list").text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.enable5,
                for: "enable5"
            }).set_class("label_header").up().text("\n                        ").child("input", {
                type: "checkbox",
                id: "enable5"
            }).bind(p).directive("bind", p).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.name,
                id: "name",
                for: "wifiName5"
            }).set_class("label_wizard").up().text("\n                        ").child("input", {
                maxlength: "16",
                id: "wifiName5",
                type: "text"
            }).set_class("input").bind(h).directive("bind", h).up().text("\n                    ").up().text("\n                    ").child("li", {}).set_class("wifi_list-item").text("\n                        ").child("label", {
                text: s.wizard.pass,
                for: "wifiPass5"
            }).set_class("label_wizard").up().text("\n                        ").child("input", {
                maxlength: "32",
                id: "wifiPass5",
                type: "password"
            }).set_class("input").bind(_).directive("bind", _).up().text("\n                    ").up().text("\n                ").up().text("\n            ").up().text("\n            ").child("div", {}).set_class("nav_buttons buttons_container").text("\n                ").child("input", {
                value: s.button.back,
                id: "back",
                type: "button"
            }).set_class("link_bg-btn").bind(o).directive("bind", o).up().text("\n                ").child("input", {
                value: s.button.manual,
                id: "exit-home",
                type: "button"
            }).set_class("link_bg-btn").bind(a).directive("bind", a).up().text("\n                ").child("input", {
                value: s.button.next,
                id: "next",
                type: "button"
            }).set_class("link_bg-btn").bind(r).directive("bind", r).up().text("\n            ").up().text("\n        ").up().text("\n    ");
        };
    }, {
        "event-emitter.js": 8,
        "multilang.js": 15,
        "virtual-dom.js": 26
    } ],
    137: [ function(t, e, n) {
        "use strict";
        var h = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function(t) {
            return typeof t;
        } : function(t) {
            return t && "function" == typeof Symbol && t.constructor === Symbol && t !== Symbol.prototype ? "symbol" : typeof t;
        }, _ = t("./nano-util.js");
        function i(c, u, d) {
            var p = performance.now();
            return new Promise(function(n, i) {
                var t = d && d.method || "POST", e = d && void 0 !== d.nocache ? d.nocache : 1, s = new XMLHttpRequest(), o = 0;
                function r() {
                    a(0), s && (s.onreadystatechange = _.noop, s.abort(), s = void 0);
                }
                function a(t) {
                    0 !== o && (o = clearTimeout(o)), t && (o = setTimeout(function() {
                        o = 0, r(), i("TIMEOUT");
                    }, t));
                }
                function l() {
                    var t = function() {
                        var t = Object.create(null);
                        if (!arguments.length) return t;
                        for (var e = 0, n = arguments, i = n.length; e < i; e += 2) t[n[e]] = n[e + 1];
                        return t;
                    }(), e = s.responseType, n = s.responseXML, i = s.responseText;
                    return t.status = s.status, e && (s.responseType = e), null !== n && (t.responseXML = n), 
                    null !== i && (t.responseText = i), t;
                }
                return s.onreadystatechange = function() {
                    var t, e;
                    4 == s.readyState && (t = s.status, e = l(), 200 <= t && t < 300 || 304 === t ? (a(0), 
                    e.etime = (performance.now() - p) / 1e3, n(e)) : (i(Error("HTTP:" + e.status)), 
                    location, r()));
                }, s.open(t, c, !0), e && s.setRequestHeader("If-Modified-Since", "Sat, 1 Jan 1996 00:00:00 GMT"), 
                s.setRequestHeader("Content-Type", /^\s*<\w+/.test(u) ? "application/xml" : "application/text"), 
                a(d && d.timeout), s.send("object" !== (void 0 === u ? "undefined" : h(u)) ? u : JSON.stringify(u)), 
                {
                    cancel: r
                };
            });
        }
        e.exports.ajaxSend = i, e.exports.getJson = function(t) {
            return i(t, null, {
                method: "GET",
                nocache: 1,
                timeout: 500
            }).then(function(t) {
                return JSON.parse(t.responseText);
            });
        };
    }, {
        "./nano-util.js": 145
    } ],
    138: [ function(t, e, n) {
        "use strict";
        var r = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function(t) {
            return typeof t;
        } : function(t) {
            return t && "function" == typeof Symbol && t.constructor === Symbol && t !== Symbol.prototype ? "symbol" : typeof t;
        }, i = (t("./nano-util.js"), t("./nano-object.js"));
        function s(t) {
            return document.createElement(t);
        }
        function o(t) {
            this.e = s(t);
        }
        function a(t) {
            this.e = document.createElementNS("http://www.w3.org/2000/svg", t);
        }
        function l(t) {
            this.e = t;
        }
        function c(t) {
            this.e = t;
        }
        function u() {
            this.attrs = O(), this.style = O(), this.list = [], this.events = O();
        }
        function d(t) {
            t && t.options ? this.e = t : (this.e = s("select"), t && this.addOptions(t));
        }
        function p(t) {
            this.e = t || s("table"), this.id = void 0, this.width = 0, this.cols = void 0, 
            this.height = 0;
        }
        function h(t) {
            this.e = t || s("tbody"), this.height = 0;
        }
        function _(t) {
            this.e = t || s("tr");
        }
        var f = {
            attr: function(t, e) {
                return void 0 !== e && (this.e[t] = e), this;
            },
            hattr: function(t, e) {
                return void 0 !== e && (null !== e ? this.e.setAttribute(t, e) : this.e.removeAttribute(t)), 
                this;
            },
            hattrNS: function(t, e, n) {
                return void 0 !== n && (null !== n ? this.e.setAttributeNS(t, e, n) : this.e.removeAttributeNS(t, e)), 
                this;
            },
            hattrs: function(t) {
                if (t) for (var e in t) void 0 !== t[e] && this.hattr(e, t[e]);
                return this;
            },
            attrs: function(t) {
                var e = this.e;
                if (t) for (var n in t) void 0 !== t[n] && (e[n] = t[n]);
                return this;
            },
            style: function(t, e) {
                return this.e.style[t] = e, this;
            },
            setClass: function(t) {
                return void 0 !== t && (this.e.className = t), this;
            },
            setSubClass: function(t, e) {
                if (!t) return this;
                var n = (this.e.className || "").split(" "), i = n.indexOf(t);
                return i < 0 ^ !e && (e ? n.push(t) : n.splice(i, 1), this.e.className = n.join(" ")), 
                this;
            },
            changeSubClass: function(t, e) {
                for (var n = this.e, i = 0 == n.className.length ? [] : n.className.split(" "), s = t.length, o = 0; o < i.length; ++o) {
                    var r = i[o];
                    if (r.slice(0, s) == t && "_" === r.charAt(s)) return e ? i[o] = [ t, e ].join("_") : i.splice(o, 1), 
                    n.className = i.join(" "), this;
                }
                return e && (i.push([ t, e ].join("_")), n.className = i.join(" ")), this;
            },
            addClass: function(t) {
                return t && (this.e.className ? this.e.className += " " + t : this.e.className = t), 
                this;
            },
            value: function(t) {
                return void 0 !== t && (this.e.value = t), this;
            },
            checked: function(t) {
                return this.e.checked = !!t, this;
            },
            disabled: function(t) {
                return t ? this.e.setAttribute("disabled", "") : this.e.removeAttribute("disabled"), 
                this;
            },
            id: function(t) {
                return "string" == typeof t && (this.e.id = t), this;
            },
            name: function(t) {
                return this.e.name = t, this;
            },
            show: function(t) {
                return t ? this.e.removeAttribute("hidden") : this.e.setAttribute("hidden", ""), 
                this;
            },
            show_if: function(t, e) {
                var n = this;
                return n.show(e(t)), t.on("click", function() {
                    return n.show(e(t));
                }), this;
            },
            disabled_if: function(t, e) {
                var n = this;
                return n.disabled(e(t)), t.on("click", function() {
                    return n.disabled(e(t));
                }), this;
            },
            onAnEvent: function(t, e) {
                return this.e.addEventListener(t, e, !1), this;
            },
            offAnEvent: function(t, e) {
                return this.e.removeEventListener(t, e, !1), this;
            },
            on: function(t, e) {
                for (var n = 0, i = (t = t.split(",")).length; n < i; ++n) this.onAnEvent(t[n], e, !1);
                return this;
            },
            off: function(t, e) {
                for (var n = 0, i = (t = t.split(",")).length; n < i; ++n) this.offAnEvent(t[n], e, !1);
                return this;
            }
        }, m = {
            attr: function(t, e) {
                try {
                    return void 0 !== e && (this.attrs[t] = e), this;
                } catch (t) {
                    return this;
                }
            },
            style: function(t, e) {
                return this.style[t] = e, this;
            },
            setClass: function(t) {
                return void 0 !== t && (this.attrs.className = t), this;
            },
            addClass: function(t) {
                return this.attrs.className ? this.attrs.className += " " + t : this.attrs.className = t, 
                this;
            },
            setSubClass: function(t, e) {
                var n = (this.attrs.className || "").split(" "), i = n.indexOf(t);
                return i < 0 ^ !e && (e ? n.push(t) : n.splice(i, 1), this.attrs.className = n.join(" ")), 
                this;
            },
            changeSubClass: function(t, e) {
                for (var n = this.attrs, i = 0 == n.className.length ? [] : n.className.split(" "), s = t.length, o = 0; o < i.length; ++o) {
                    var r = i[o];
                    if (r.slice(0, s) == t && "_" === r.charAt(s)) return e ? i[o] = [ t, e ].join("_") : i.splice(o, 1), 
                    n.className = i.join(" "), this;
                }
                return e && (i.push([ t, e ].join("_")), n.className = i.join(" ")), this;
            },
            value: function(t) {
                return void 0 !== t && (this.attrs.value = t), this;
            },
            checked: function(t) {
                return this.attrs.checked = !!t, this;
            },
            id: function(t) {
                return this.attrs.id = t, this;
            },
            name: function(t) {
                return this.attrs.name = t, this;
            },
            on: function(t, e) {
                for (var n = 0, i = t.split(","), s = this.events, o = i.length; n < o; ++n) {
                    var r = i[n];
                    (s[r] || (s[r] = [])).push(e);
                }
            }
        }, v = i.extend(f, {
            add: function(t) {
                switch (void 0 === t ? "undefined" : r(t)) {
                  case "number":
                  case "boolean":
                  case "string":
                    var e = document.createTextNode(t);
                    break;

                  case "object":
                    if (t) {
                        if (t.toDOM && "string" == typeof (t = t.toDOM())) {
                            e = document.createTextNode(t);
                            break;
                        }
                        if (t.applyTo) return t.applyTo(this), this;
                        e = t.e || t;
                        break;
                    }

                  default:
                    return this;
                }
                return this.e.appendChild(e), this;
            },
            insert: function(t, e) {
                return void 0 !== t && "null" !== t && this.e.insertBefore("object" === (void 0 === t ? "undefined" : r(t)) ? t.toDOM ? t.toDOM().e : t.e || t : document.createTextNode(t), "number" == typeof e ? this.e.childNodes[e || 0] : e), 
                this;
            },
            html: function(t) {
                return this.e.innerHTML = t, this;
            },
            empty: function() {
                return this.e.innerHTML = "", this;
            },
            set: function(t) {
                return this.empty().add(t);
            },
            del: function() {
                return this.e.parentNode && this.e.parentNode.removeChild(this.e), this;
            },
            takeAwayChildren: function(t) {
                for (var e = t.e, n = this.e; e.firstChild; ) n.appendChild(e.firstChild);
                return this;
            },
            alignToRect: function(t, e, n, i, s) {
                var o = 0, r = 0, a = this.e, l = 0, c = 0, u = a.offsetWidth, d = a.offsetHeight;
                switch (t) {
                  case "lefter":
                    o = s.left - u, n *= -1;
                    break;

                  case "left":
                    o = s.left - l;
                    break;

                  case "right":
                    o = s.right - u, n *= -1;
                    break;

                  case "righter":
                    o = s.right - l;
                    break;

                  case "center":
                  default:
                    o = (s.right + s.left - (u + l)) / 2;
                }
                switch (e) {
                  case "upper":
                    r = s.top - d, i *= -1;
                    break;

                  case "top":
                    r = s.top - c;
                    break;

                  case "bottom":
                    r = s.bottom - d, i *= -1;
                    break;

                  case "lower":
                    r = s.bottom - c;
                    break;

                  case "middle":
                  default:
                    r = (s.bottom + s.top - (d + c)) / 2;
                }
                return this.move(o + n, r + i);
            },
            alignToSibling: function(t, e, n, i, s) {
                var o = this.e;
                if (0 < t) for (;(o = o.nextSibling) && --t; ) ; else for (;(o = o.previousSibling) && ++t; ) ;
                if (!o) return this;
                var r = {
                    left: o.offsetLeft,
                    top: o.offsetTop
                };
                return r.right = r.left + o.offsetWidth, r.bottom = r.top + o.offsetHeight, this.alignToRect(e, n, i, s, r);
            },
            alignToParent: function(t, e, n, i) {
                var s = this.e.parentNode;
                this.alignToRect(t, e, 0 | n, 0 | i, {
                    left: 0,
                    top: 0,
                    right: s.offsetWidth,
                    bottom: s.offsetHeight
                });
            },
            getBoundingClientRect: function() {
                var t = this.e.getBoundingClientRect(), e = window.scrollX, n = window.scrollY;
                return {
                    left: t.left + e,
                    top: t.top + n,
                    right: t.right + e,
                    bottom: t.bottom + n
                };
            },
            move: function(t, e) {
                return this.e.style.left = 0 != t ? (0 | t) + "px" : "0", this.e.style.top = 0 != e ? (0 | e) + "px" : "0", 
                [ t, e ];
            },
            self_align: function(t, e) {
                var n = g(this.e.parentNode).getBoundingClientRect();
                n.right -= n.left, n.bottom -= n.top, n.left = 0, n.top = 0, this.align(n, t, e, 0, 0);
            }
        }), t = i.extend(m, {
            add: function(t) {
                return void 0 !== t && this.list.push(t), this;
            },
            insert: function(t, e) {
                return void 0 !== t && this.list.splice(e || 0, 0, t), this;
            },
            html: function(t) {
                return this.empty(), this.attrs.innerHTML = t, this;
            },
            empty: function() {
                return this.list = [], this;
            },
            set: function(t) {
                return this.list = [ t ], this;
            },
            del: function() {
                return this;
            },
            applyTo: function(t) {
                var e = t.e, n = this.attrs, i = this.style, s = this.events, o = e.style;
                for (l in n) {
                    var r = n[l];
                    switch (l) {
                      case "innerHTML":
                        for (var a = g.div().html(r).e; a.firstChild; ) e.appendChild(a.firstChild);
                        break;

                      case "className":
                        if (e.className) {
                            e.className = [ e.className, r ].join(" ");
                            break;
                        }

                      default:
                        0 <= l.indexOf("-") ? e.setAttribute(l, r) : e[l] = r;
                    }
                }
                for (l in i) o[l] = i[l];
                for (var l = 0, c = this.list, u = c.length; l < u; ++l) t.add(c[l]);
                for (l in s) t.on(l, s[l]);
                return this;
            }
        }), m = i.extend(v, {
            up: function() {
                return this.e = this.e.parentNode, this;
            },
            down: function(t) {
                var e = this.e.childNodes;
                return this.e = e[t < 0 ? t + e.length : t], this;
            },
            next: function() {
                return this.e = this.e.nextSibling, this;
            },
            prev: function() {
                return this.e = this.e.previousSibling, this;
            },
            dup: function() {
                return new l(this.e);
            }
        });
        l.prototype = a.prototype = o.prototype = m, u.prototype = t, c.prototype = i.extend(v, {
            fire: function(t) {
                var e = document.createEvent("UIEvents");
                e.initUIEvent(t, !0, !1, window, 0), this.e.dispatchEvent(e);
            }
        }), d.prototype = i.extend(v, {
            addOptions: function(t) {
                if (t instanceof Array) for (var e = 0, n = t.length; e < n; ++e) {
                    var i = t[e];
                    this.add(new o("option").add(i.text).attr("value", i.value));
                } else for (var s in t) this.add(new o("option").add(t[s]).attr("value", s));
                return this;
            },
            addOption: function(t, e) {
                this.add(new o("option").add(e).attr("value", t));
            }
        }), p.prototype = i.extend(f, {
            newHead: function() {
                return this.thead || (this.thead = this.e.createTHead()), new _(this.thead.insertRow(-1));
            },
            newRow: function() {
                return this.tbody || this.e.appendChild(this.tbody = document.createElement("tbody")), 
                ++this.height, new _(this.tbody.insertRow(-1));
            },
            newBody: function() {
                var t = document.createElement("tbody");
                return this.e.appendChild(t), new h(t);
            },
            empty: function() {
                for (var t = this.e, e = t.tBodies; e.length; ) t.removeChild(e[0]);
                this.height = 0;
            },
            newColgroup: function() {
                var t = document.createElement("colgroup");
                return this.e.appendChild(t), new l(t);
            },
            newCaption: function(t) {
                return g.el(this.e.createCaption()).add(t);
            }
        }), h.prototype = i.extend(f, {
            newRow: function() {
                return ++this.height, new _(this.e.insertRow(-1));
            }
        }), _.prototype = i.extend(v, {
            newCell: function() {
                return new l(this.e.insertCell(-1));
            },
            newHeadCell: function() {
                var t = new o("th");
                return this.add(t), t;
            }
        });
        var b, g = e.exports = function(t) {
            return new l(t);
        };
        g.del = function(t) {
            t.parentNode && t.parentNode.removeChild(t);
        }, g.body = function() {
            return new l(document.body);
        }, g.window = function() {
            return new c(window);
        }, g.dom = function(t) {
            t = document.getElementById(t);
            return t ? new l(t) : void 0;
        }, g.el = function(t) {
            return new l(t);
        }, g.toEl = function(t) {
            return new l(t = "string" == typeof t ? document.createTextNode(t) : t);
        }, g.tag = function(t) {
            return new o(t);
        }, g.proxy = function() {
            return new u();
        }, g.iframe = function(t) {
            return new o("iframe").id(t).name(t);
        }, "div,ul,li,dl,nav,aside,section,article,header,main".split(",").forEach(function(e) {
            g[e] = function(t) {
                return new o(e).setClass(t);
            };
        }), "h1,h2,h3,h4,h5,h6,p,i,b,span,small,dt,dd".split(",").forEach(function(e) {
            g[e] = function(t) {
                return new o(e).add(t);
            };
        }), g.a = g.link = function(t, e) {
            return new o("a").add(e).attr("href", t);
        }, g.html = function(t) {
            return new u().html(t);
        }, g.div = function() {
            return new o("div");
        }, g.span = function() {
            return new o("span");
        }, g.label = function(t, e) {
            if ("object" !== (void 0 === e ? "undefined" : r(e))) return new o("label").attr("htmlFor", e).add(t);
            var n = e.e.id || this.id(e.e.name);
            return g.proxy().add(new o("label").attr("htmlFor", n).add(t)).add(e.id(n));
        }, g.input = function(t) {
            return new o("input").attr("type", t);
        }, g.button = function(t, e) {
            return new o("button").add(t).attr("type", e);
        }, g.select = function(t) {
            return new d(t);
        }, b = 10, g.uid = function() {
            return "$" + (b++).toString(36);
        }, g.table = function(t, e, n) {
            if ("object" === (void 0 === t ? "undefined" : r(t))) return new p(t);
            var i = new p();
            if (i.setClass(e), t && i.attr("id", t), i.id = t, "number" == typeof n) {
                i.width = n;
                for (var s = i.cols = [], o = 0; o < n; ++o) s[o] = [ t, o ].join("_");
            } else {
                if (!(n instanceof Array)) {
                    s = n;
                    for (o in n = [], s) n.push(o);
                }
                i.cols = n, i.width = n.length;
            }
            return i.height = 0, i;
        }, g.svg = function() {
            return new a("svg");
        }, g.svgtag = function(t) {
            return new a(t);
        }, g.symbol = function(t, e) {
            return new a("symbol").id(t).hattr("viewBox", e);
        }, g.use = function(t) {
            return new a("use").hattrNS("http://www.w3.org/1999/xlink", "href", t);
        }, g.marker = function(t) {
            return new a("marker").attr("id", t);
        }, [ "defs", "circle", "path", "rect", "linearGradient", "stop", "image", "ellipse", "clipPath", "polygon", "animateTransform", "animate" ].forEach(function(t) {
            g[t] = function() {
                return new a(t);
            };
        }), [ "text", "tspan" ].forEach(function(e) {
            g[e] = function(t) {
                return new a(e).add(t);
            };
        }), g.g = function(t) {
            return new a("g").hattr("class", t);
        }, g.vicon = function(t, e) {
            return g.svg().hattrNS(null, "class", (e || "icon") + " " + t).add(g.use("#" + t));
        };
    }, {
        "./nano-object.js": 140,
        "./nano-util.js": 145
    } ],
    139: [ function(t, e, n) {
        "use strict";
        var a = t("./nano-ajax.js"), l = t("./os.js");
        function c() {
            var i = [], s = 0, t = function() {
                var t = Object.create(null);
                if (!arguments.length) return t;
                for (var e = 0, n = arguments, i = n.length; e < i; e += 2) t[n[e]] = n[e + 1];
                return t;
            }();
            return t.add = function(t, e) {
                var n = i.length;
                return i.push('{"jsonrpc":"2.0","id":"' + n + '","method":"' + t + '","params":' + JSON.stringify(e) + "}"), 
                ++s, n;
            }, t.cancel = function(t) {
                return i[t] = 0, --s;
            }, t.join = function() {
                return s && "[" + i.filter(function(t) {
                    return t;
                }).join(",") + "]";
            }, t.isEmpty = function() {
                return i.length;
            }, t;
        }
        var o = {}, r = {};
        function i(n) {
            n in o || (o[n] = {
                q: c()
            }), n in r || (r[n] = {
                req: void 0
            });
            var i = o[n], s = r[n];
            return function(t, e) {
                return function(t, s, e, n, i) {
                    n.q.isEmpty() || (i.req = l.timer(11).then(function() {
                        var t = n.q.join();
                        return t ? (n.q = c(), a.ajaxSend(e, t).then(function(t) {
                            try {
                                for (var e = JSON.parse(t.responseText), n = [], i = 0, s = e.length; i < s; ++i) {
                                    var o = e[i];
                                    n[o.id] = o;
                                }
                                return n;
                            } catch (t) {
                                location;
                            }
                        })) : [];
                    }));
                    var o = n.q, r = i.req;
                    return new Promise(function(e, n) {
                        var i = o.add(t, s);
                        return r.then(function(t) {
                            t = t[i];
                            0 !== t && (t || console.error("invalid RPC responce id: '%s'", i), t.result ? e(t.result) : t.error && n(t.error));
                        }, n), {
                            cancel: function() {
                                o.cancel(i) || r.cancel();
                            }
                        };
                    });
                }(t, e, n, i, s);
            };
        }
        (e.exports = i("/rpcform/jsonrpc")).login_json_rpc = i("/rpcform/login");
    }, {
        "./nano-ajax.js": 137,
        "./os.js": 146
    } ],
    140: [ function(t, e, a) {
        "use strict";
        var d = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function(t) {
            return typeof t;
        } : function(t) {
            return t && "function" == typeof Symbol && t.constructor === Symbol && t !== Symbol.prototype ? "symbol" : typeof t;
        };
        a.isEmpty = function(t) {
            for (var e in t) return;
            return 1;
        }, a.isEqual = function(t, e) {
            return function t(e, n) {
                var i = void 0 === e ? "undefined" : d(e);
                if (i !== (void 0 === n ? "undefined" : d(n))) return !1;
                switch (i) {
                  case "string":
                  case "number":
                  case "boolean":
                    return e === n;

                  case "object":
                    if (!(e instanceof Array && n instanceof Array)) {
                        var s = Object.keys(n), o = Object.keys(e);
                        if (o.length !== s.length) return;
                        for (var r = 0, a = o.length; r < a; ++r) {
                            var l = o[r];
                            if (s[r] !== l || !t(e[l], n[l])) return !1;
                        }
                        return r === s.length;
                    }
                    if (e.length !== n.length) return !1;
                    for (var r = 0, c = e.length; r < c; ++r) if (!t(e[r], n[r])) return !1;
                }
                return !0;
            }(t, e);
        }, a.clone = function(t) {
            if (!t || "object" !== (void 0 === t ? "undefined" : d(t))) return t;
            for (var e = t instanceof Array ? [] : Object.create(Object.getPrototypeOf(t)), n = Object.getOwnPropertyNames(t), i = 0, s = n.length; i < s; ++i) {
                var o = n[i], r = Object.getOwnPropertyDescriptor(t, o);
                r.value && "object" === d(r.value) && (r.value = a.clone(r.value)), Object.defineProperty(e, o, r);
            }
            return e;
        }, a.dup = function(t) {
            if (!t || "object" !== (void 0 === t ? "undefined" : d(t))) return t;
            for (var e = t instanceof Array ? [] : Object.create(Object.getPrototypeOf(t)), n = Object.getOwnPropertyNames(t), i = 0, s = n.length; i < s; ++i) {
                var o = n[i];
                Object.defineProperty(e, o, Object.getOwnPropertyDescriptor(t, o));
            }
            return e;
        }, a.duprec = function(t) {
            if (!t || "object" !== (void 0 === t ? "undefined" : d(t))) return t;
            for (var e = O(), n = Object.keys(t), i = 0, s = n.length; i < s; ++i) {
                var o = n[i];
                e[o] = t[o];
            }
            return e;
        }, a.mix = function(e, n) {
            return n && Object.keys(n).forEach(function(t) {
                e[t] = n[t];
            }), e;
        }, a.mixin = function(e, n) {
            return Object.keys(n).forEach(function(t) {
                void 0 === e[t] && (e[t] = n[t]);
            }), e;
        }, a.extend = function(t, e) {
            return a.mix(a.dup(t), e);
        }, a.injectTo = function(t) {
            var e = "function" == typeof t ? t.prototype : t, t = this.prototype;
            object.mixin(e, t);
        }, a.diff = function(t, e, n) {
            var i = void 0 === t ? "undefined" : d(t);
            if (i === (void 0 === e ? "undefined" : d(e)) && "object" === i) {
                for (var s = Object.keys(t), o = Object.keys(e), r = O(), a = 0, l = s.length; a < l; ++a) r[s[a]] = 1;
                for (a = 0, l = o.length; a < l; ++a) r[o[a]] = (r[o[a]] || 0) + 1;
                for (var c = Object.keys(r), a = 0, l = c.length; a < l; ++a) {
                    var u = c[a];
                    1 !== r[u] && t[u] === e[u] || n(u, e[u], t[u]);
                }
            }
        }, a.unpackKeys = function(t) {
            for (var e = Object.keys(t), n = /\s*,\s*/, i = 0, s = e.length; i < s; ++i) {
                var o = e[i];
                if (0 <= o.indexOf(",")) {
                    for (var r = t[o], a = o.split(n), l = 0, c = a.length; l < c; ++l) t[a[l]] = r;
                    delete t[o];
                }
            }
            return t;
        };
        var p = a.unpackTree = function(t, e) {
            var n = Object.keys(t), i = /\s*,\s*/;
            e = e || ret;
            for (var s = 0, o = n.length; s < o; ++s) {
                var r = n[s], a = t[r];
                if (0 <= r.indexOf(",")) {
                    for (var l = r.split(i), c = 0, u = l.length; c < u; ++c) t[l[c]] = a;
                    delete t[r];
                }
                a && "object" === (void 0 === a ? "undefined" : d(a)) ? p(a) : a = e(a);
            }
        };
    }, {} ],
    141: [ function(t, e, n) {
        "use strict";
        var l = t("./nano-promise");
        window.Promise = l;
        var c, u = [], d = 1;
        n.create = function(t, e, n) {
            var i, s, o, r, a = (i = void 0 !== n ? n : u.length, t = t / 250 + .5 | 0 || 1, 
            s = e, (e = new l(function(e, n) {
                o = function() {
                    if (!r) {
                        r = 1;
                        try {
                            var t = l.resolve(s(e, n));
                            return t.catch(function(t) {
                                t !== l.CANCEL_REASON && console.error("poll-" + i + " rejected", t);
                            }), t.finally(function() {
                                r = 0;
                            }), t;
                        } catch (t) {
                            console.error("poll-" + i + " crashed", t), n(t);
                        }
                    }
                };
            })).id = i, e.period = t, e.tick = o, e);
            return u.push(a), a.finally(function() {
                for (var t = 0, e = u.length; t < e; ++t) if (u[t] === a) {
                    u.splice(t, 1);
                    break;
                }
                u.length || clearInterval(c);
            }), 1 === u.length && u.length && (c = setInterval(function() {
                for (var t = 0, e = u.length; t < e; ++t) {
                    var n = u[t];
                    d % n.period || n.tick();
                }
                ++d;
            }, 250)), a;
        }, n.cancelAll = function() {
            for (var t = 0, e = u.length; t < e; ++t) u[t].cancel();
        };
    }, {
        "./nano-promise": 142
    } ],
    142: [ function(t, e, n) {
        "use strict";
        var u = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function(t) {
            return typeof t;
        } : function(t) {
            return t && "function" == typeof Symbol && t.constructor === Symbol && t !== Symbol.prototype ? "symbol" : typeof t;
        }, i = t("./nano-tick.js").next, r = [];
        function a() {
            Array.call(this), this.push.apply(this, arguments);
        }
        function d() {
            var t = new a();
            return t.push.apply(t, arguments), t;
        }
        function p(t, e, n) {
            r.length || i(function() {
                for (var t = 0; t < r.length; t += 3) {
                    var e = r[t], n = r[t + 1], i = r[t + 2], s = e[n ? 2 : 3];
                    if ("function" != typeof s) e[n ? 0 : 1].apply(void 0, i); else try {
                        var o = s.apply(void 0, i);
                        o instanceof a ? e[0].apply(void 0, o) : e[0](o);
                    } catch (t) {
                        e[1](t);
                    }
                }
                r = [];
            }), r.push(t, e, n);
        }
        function h(t, e, n) {
            if (t && ("object" === (void 0 === t ? "undefined" : u(t)) || "function" == typeof t)) {
                try {
                    var i = t.then;
                } catch (t) {
                    return n && n(t), 1;
                }
                if ("function" == typeof i) {
                    var s = 0;
                    try {
                        i.call(t, function() {
                            !s++ && e && e.apply(void 0, arguments);
                        }, function() {
                            !s++ && n && n.apply(void 0, arguments);
                        });
                    } catch (t) {
                        !s++ && n && n(t);
                    }
                    return 1;
                }
            }
        }
        function _(s, o) {
            return function(n, i) {
                return new v(function(t, e) {
                    p([ t, e, n, i ], s, o);
                });
            };
        }
        a.prototype = Object.create(Array.prototype);
        var f = "CANCEL";
        function m(t, n, i) {
            if ("object" !== (void 0 === t ? "undefined" : u(t)) || !("length" in t)) return i(TypeError("not array"));
            var s = 0, o = [], r = [];
            for (var e = 0, a = t.length; e < a; ++e) !function(e, t) {
                h(t, function(t) {
                    r[e] = 1 < arguments.length ? d.apply(null, arguments) : t, --s || n(r);
                }, i) ? (++s, o.push(t)) : r[e] = t;
            }(e, t[e]);
            return a && s ? {
                cancel: function() {
                    for (var t = 0, e = o.length; t < e; ++t) try {
                        o[t].cancel();
                    } catch (t) {}
                }
            } : n(r);
        }
        var v = e.exports = function(t) {
            function i(t, e) {
                if (++c, l.length) for (var n = 0; n < l.length; ++n) p(l[n], e, t); else e || t[0] === f || console.error("unhandled promise rejection", t[0]);
                a.then = _(e, t), l = 0;
            }
            function s(t) {
                c || (++c, t === f && r && r(), i(d.apply(null, arguments), 0));
            }
            function o(t) {
                if (t === a) throw TypeError();
                var e, n = d.apply(null, arguments);
                1 < n.length ? (e = m(n, function(t) {
                    i(t, 1);
                }, s)) && (r = e.cancel) : h(t, o, s) ? "function" == typeof (t = t.cancel) && (r = t) : i(n, 1);
            }
            var r, a = this, l = [], c = 0;
            if (this.then = function(n, i) {
                return new v(function(t, e) {
                    return l.push([ t, e, n, i ]), {
                        cancel: a.cancel
                    };
                });
            }, this.cancel = function() {
                return s(f), a;
            }, "function" == typeof t) try {
                var e, n = t(o, s);
                "object" !== (void 0 === n ? "undefined" : u(n)) || "function" == typeof (e = n.cancel) && (r = e);
            } catch (t) {
                s(t);
            }
        };
        v.prototype = {
            cancel: void 0,
            catch: function(t) {
                return this.then(0, t);
            },
            finally: function(e) {
                return this.then(function() {
                    return e(), d.apply(null, arguments);
                }, function(t) {
                    throw e(), t;
                });
            },
            spread: function(e, t) {
                return this.then(function(t) {
                    return e.apply(null, t instanceof Array ? t : arguments);
                }, t);
            },
            report: function(t) {
                return this.then(function() {
                    return console.log("Resolved:", void 0 === t ? Array.prototype.splice.call(arguments, 0) : arguments[t]), 
                    d.apply(null, arguments);
                }, function(t) {
                    return console.log("Rejected:", t), d.apply(null, arguments);
                });
            },
            finish: function() {
                this.catch(function(t) {
                    console.error(t);
                });
            }
        }, v.all = function(n) {
            return new v(function(t, e) {
                return m(n, t, e);
            });
        }, v.race = function(a) {
            return new v(function(t, e) {
                if ("object" !== (void 0 === a ? "undefined" : u(a)) || !1 in a) return e(TypeError("not array"));
                for (var n = [], i = function() {
                    for (var t = 0, e = n.length; t < e; ++t) try {
                        n[t].cancel();
                    } catch (t) {}
                }, s = 0, o = a.length; s < o; ++s) {
                    var r = a[s];
                    if (!h(r, t, e)) {
                        for (t(a[s]); s < o; ++s) h(r = a[s]) && r.cancel();
                        return i();
                    }
                    n.push(r), r.then(i, function(t) {
                        t !== f && i();
                    });
                }
                if (n.length) return {
                    cancel: i
                };
                t();
            });
        }, v.resolve = function() {
            var n = d.apply(null, arguments);
            return new v(function(t, e) {
                t.apply(null, n);
            });
        }, v.reject = function() {
            var t = Object.create(v.prototype);
            return t.then = _(0, d.apply(null, arguments)), t;
        }, v.Arguments = a, v.CANCEL_REASON = f, v.concat = function(t) {
            return v.all(t).then(function(t) {
                for (var e = new a(), n = 0, i = t.length; n < i; ++n) {
                    var s = t[n];
                    s instanceof a || s instanceof Array ? e.push.apply(e, s) : e.push(s);
                }
                return e;
            });
        };
    }, {
        "./nano-tick.js": 143
    } ],
    143: [ function(t, e, n) {
        "use strict";
        var i = [], s = [];
        function o() {
            for (var t = 0, e = 0; t < i.length || e < s.length; ) {
                for (;t < i.length; ++t) try {
                    i[t]();
                } catch (t) {
                    console.error(t);
                }
                for (;e < s.length; ++e) {
                    try {
                        s[e]();
                    } catch (t) {
                        console.error(t);
                    }
                    if (t < i.legth) break;
                }
            }
            i = [], s = [];
        }
        var r, a, l, c, u = window.MutationObserver;
        c = u ? (r = document.createTextNode(""), a = 0, new u(o).observe(r, {
            characterData: !0
        }), function() {
            i.length || (r.data = 1 & ++a);
        }) : (l = window.setImmediate || window.setTimeout, function() {
            i.length || l(o);
        }), n.next = function(t) {
            c(), i.push(t);
        }, n.afterNext = function(t) {
            c(), s.push(t);
        };
    }, {} ],
    144: [ function(t, e, n) {
        "use strict";
        e.exports = function(n, i) {
            var s, o, i = Array.prototype.splice.call(arguments, 1), r = new Promise(function(t, e) {
                return s = setTimeout(o = function() {
                    r.restart = function() {}, t.apply(null, i);
                }, n), {
                    cancel: function() {
                        clearTimeout(s);
                    }
                };
            });
            return r.restart = function() {
                clearTimeout(s), s = setTimeout(o, n);
            }, r;
        };
    }, {} ],
    145: [ function(t, e, n) {
        "use strict";
        var i;
        e.exports.time = function() {
            return new Date().getTime();
        }, e.exports.timestamp = (i = new Date().getTime(), function() {
            return new Date().getTime() - i;
        }), e.exports._false = function() {
            return !1;
        }, e.exports._true = function() {
            return !0;
        }, e.exports.noop = function() {}, e.exports.ret = function(t) {
            return t;
        };
    }, {} ],
    146: [ function(t, e, n) {
        "use strict";
        var i = t("./nano-timer.js"), s = t("./nano-tick.js"), t = t("./nano-poller.js");
        e.exports.nextTick = s.next, e.exports.afterNextTick = s.afterNext, e.exports.timer = i, 
        e.exports.poll = t.create, e.exports.cancelAllPolls = t.cancelAll;
    }, {
        "./nano-poller.js": 141,
        "./nano-tick.js": 143,
        "./nano-timer.js": 144
    } ],
    147: [ function(e, t, n) {
        "use strict";
        t.exports.registry = function(t) {
            t.registry("page-pending", e("page-pending.vd").Ctor), t.registry("global-notify", e("global-notify.vd").Ctor);
        };
    }, {
        "global-notify.vd": 148,
        "page-pending.vd": 149
    } ],
    148: [ function(h, t, e) {
        "use strict";
        var _ = h("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = h("multilang.js").lang, i = h("event-emitter.js").EventEmiter, s = (h("navi.js").navi, 
            void 0), o = h("dots-pending.js"), r = o.DotsPending, a = o.timeCountDown, l = n(), e = e.attr, c = (e.text, 
            e.name, e.topName, this.exports = {}), u = (new i(), {}), d = {}, p = {};
            this.obj = {
                created: function() {
                    d.el.show(!1), c.countDown = function(t, e, n) {
                        d.el.show(!0), new a(p.el, t, e, function(t) {
                            d.el.show(!1), n();
                        });
                    }, c.run = function() {
                        u.el.setClass("notify-content notify-content-info"), d.el.show(!0), s = new r(p.el, l.pending.applying);
                    }, c.done = function() {
                        u.el.setClass("notify-content"), d.el.show(!1), s && s.stop();
                    }, c.error = function(t) {
                        s && s.stop(), p.el.set(t), u.el.setClass("notify-content notify-content-error"), 
                        d.el.show(!0);
                    }, c.stop = function() {
                        d.el.setClass("notify-content"), d.el.show(!1), s && s.stop();
                    };
                },
                mounted: function() {}
            }, this.tree = new _("div", {}), this.tree.root().set_class("global-notify").child("div", {}).set_class("notify-content").bind(u).directive("bind", u).child("div", {}).set_class("notify-text").child("div", {}).bind(d).directive("bind", d).child("label", {
                text: l.pending.applying
            }).set_class('""').bind(p).directive("bind", p).up().up().up().child("div", {}).set_class("notify-close-ico").up().up();
        };
    }, {
        "dots-pending.js": 6,
        "event-emitter.js": 8,
        "multilang.js": 15,
        "navi.js": 16,
        "virtual-dom.js": 26
    } ],
    149: [ function(d, t, e) {
        "use strict";
        var p = d("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = d("multilang.js").lang, i = (d("system.js").poll, d("dots-pending.js")), s = i.DotsPending, o = i.timeCountDown, r = n(), a = (e.attr, 
            {}), l = void 0, c = {}, u = this.exports = {};
            this.obj = {
                created: function() {
                    a.el.show(!1), u.countDown = function(t, e, n) {
                        a.el.show(!0), new o(c.el, t, e, function(t) {
                            a.el.show(!1), n();
                        });
                    }, u.run = function() {
                        a.el.show(!0), l = new s(c.el, r.pending.applying);
                    }, u.stop = function() {
                        a.el.show(!1), l && l.stop();
                    };
                }
            }, this.tree = new p("div", {}), this.tree.root().set_class('""').bind(a).directive("bind", a).child("label", {
                text: r.pending.applying
            }).set_class('""').bind(c).directive("bind", c).up();
        };
    }, {
        "dots-pending.js": 6,
        "multilang.js": 15,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    150: [ function(t, e, n) {
        "use strict";
        var s = t("system.js").$, o = t("system.js").app;
        function i(t) {
            o.navi().set_root("app.html").default(function() {
                return 0;
            }), (this.routes = t).forEach(function(t) {
                var n = t.href || t.root;
                o.navi().href(t.name, n), t.children && t.children.forEach(function(t) {
                    var e = t.href || t.path, e = n + "?" + e;
                    o.navi().href(t.path, e);
                });
            }), this.start();
        }
        i.prototype.getRoute = function(e) {
            var t = this.routes.find(function(t) {
                return "" != t.root && e.match(t.root);
            });
            if (!t) return {
                root: {
                    hasMenu: !0,
                    isNotSpa: !0
                }
            };
            if (!t.children) return {
                root: t
            };
            var n = t.children.find(function(t) {
                return "" != t.path && e.match(t.path);
            });
            return {
                root: t,
                route: n
            };
        }, i.prototype.start = function() {
            s.dom("app").show(!1), s.dom("page") && s.dom("page").show(!1);
            var t = "app", e = this.getRoute(window.location.href);
            if (!e) return console.error("Router: root not found!"), void (s.dom("page") && s.dom("page").show(!0));
            var n = (e.route || e.root).hasMenu, i = (e.route || e.root).isNotSpa;
            if ((n || i) && (n = o.RM.render_component("app"), o.RM.mount(n, t), t = "content", 
            o.menu().update(window.location.href)), i) return s.dom("content").add(s.dom("page")), 
            s.dom("page") && s.dom("page").show(!0), void s.dom("app").show(!0);
            e = e.route ? e.route.component : e.root.default;
            return "function" == typeof e ? e(t) : (e = o.RM.render_component(e), o.RM.mount(e, t)), 
            s.dom("page") && s.dom("page").show(!0), s.dom("app").show(!0), this;
        }, e.exports.Router = i;
    }, {
        "system.js": 23
    } ],
    151: [ function(t, e, n) {
        "use strict";
        var i = t("system.js").app, s = t("../../spa/js/client_table.js"), o = t("../../spa/js/ipfilter.js"), r = t("../../spa/js/spa-vd-wlactrl.js");
        var a = [], t = {
            root: "app.html",
            name: "app",
            default: function() {
                window.location.href = "status.html";
            },
            children: []
        };
        "BEELINE" == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_CUSTOMER && (t.default = "main"), 1 == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_LUNA ? "RTC" == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_CUSTOMER ? t.children.push({
            path: "wizard",
            component: function() {
                i.wizard ? i.wizard().run("app", {
                    go_home: function() {
                        window.location.href = "status.html";
                    },
                    success: function() {
                        window.location.href = "http://rt.ru/";
                    }
                }) : console.error("WIZARD IS NOT IMPLIMENTED");
            }
        }) : "BEELINE" == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_CUSTOMER && (t.children.push({
            path: "main",
            component: "main"
        }), t.children.push({
            path: "quick-config",
            component: "quick-config"
        }), t.children.push({
            path: "netmap",
            component: "netmap"
        }), t.children.push({
            path: "about",
            component: "about"
        })) : (t.default = function() {
            window.location.href = "status.htm";
        }, t.children.push({
            path: "client_table",
            component: s.render
        }), t.children.push({
            path: "ipfilter",
            component: o.render
        }), t.children.push({
            path: "macfilter",
            component: "mac-black-list"
        }), t.children.push({
            path: "macwhite",
            component: "mac-white-list"
        }), t.children.push({
            path: "urlfilter",
            component: "url-filter"
        }), t.children.push({
            path: "unity_status",
            component: "unity-status"
        }), t.children.push({
            path: "change_password",
            component: "change-password"
        }), t.children.push({
            path: "udpxy",
            component: "udpxy"
        }));
        o = !!{
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_LUNA;
        !{
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_USER_REMOTE_ACCESS_TBL ? a.push({
            isNotSpa: !0,
            hasMenu: !0,
            name: "acl",
            root: "acl.html"
        }) : t.children.push({
            path: "acl",
            component: "acl",
            hasMenu: o
        });
        o = {
            root: "login.html",
            name: "login",
            default: "login",
            children: []
        };
        "BEELINE" == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_CUSTOMER && (o.children.push({
            path: "bee-auth",
            component: "bee-auth"
        }), o.children.push({
            path: "bee-welcome",
            component: "bee-welcome"
        })), a.push(t), a.push(o), 1 == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_LUNA && ("BEELINE" == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_CUSTOMER && (o.default = "bee-welcome"), a.push({
            isNotSpa: !0,
            hasMenu: !0,
            name: "stats",
            root: "stats.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "status",
            root: "status.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "home",
            root: "status.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "upgrade",
            root: "upgrade.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "dhcptbl",
            root: "dhcptbl.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "bridging",
            root: "bridging.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "multi_ap_setting_general",
            root: "multi_ap_setting_general.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "managment",
            root: "upgrade.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "dms",
            root: "dms.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "samba",
            root: "samba.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "udpxy",
            root: "udpxy.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "reboot",
            root: "reboot.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "syslog",
            root: "syslog.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "tr069config",
            root: "tr069config.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "password",
            root: "password.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "saveconf",
            root: "saveconf.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "tz",
            root: "tz.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "routetbl",
            root: "routetbl.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "ddns",
            root: "ddns.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "additional",
            root: "ddns.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "qos_traffic",
            root: "net_qos_traffictl.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "qos_cls",
            root: "net_qos_cls.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "qos_imq_policy",
            root: "net_qos_imq_policy.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "routing",
            root: "routing.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "firewall",
            root: "fw-portfw.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "portfw",
            root: "fw-portfw.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "fw-ipportfilter",
            root: "fw-ipportfilter.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "fw-macfilter",
            root: "fw-macfilter.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "dmz",
            root: "fw-dmz.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "url_blocking",
            root: "url_blocking.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "algonoff",
            root: "luna-alg/algonoff.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "dos",
            root: "dos.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlan2",
            href: "/boaform/admin/formWlanRedirect?redirect-url=/wlbasic.html&wlan_idx=1"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlan5",
            href: "/boaform/admin/formWlanRedirect?redirect-url=/wlbasic.html&wlan_idx=0"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlbasic",
            root: "wlbasic.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wladvanced",
            root: "wladvanced.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlmultipleap",
            root: "wlmultipleap.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlwpa",
            root: "wlwpa.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlwds",
            root: "wlwds.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlbasic",
            root: "wlbasic.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlft",
            root: "wlft.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlwps",
            root: "wlwps.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlsurvey",
            root: "wlsurvey.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "wlactrl",
            root: "wlactrl.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "settings",
            root: "multi_wan_generic.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "lancfg",
            root: "lancfg.html"
        }, {
            isNotSpa: !0,
            hasMenu: !0,
            name: "multi_wan_generic",
            root: "multi_wan_generic.html"
        })), {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_LUNA || a.push({
            root: "wlactrl.htm",
            name: "wlactrl",
            default: r.render
        }), e.exports.routes = a;
    }, {
        "../../spa/js/client_table.js": 153,
        "../../spa/js/ipfilter.js": 155,
        "../../spa/js/spa-vd-wlactrl.js": 161,
        "system.js": 23
    } ],
    152: [ function(t, e, n) {
        "use strict";
        var i, s = t("virtual-dom.js").RenderMachine, o = t("virtual-dom.js").ComponentHub, r = t("system.js").$, a = t("system.js").app, l = t("multilang.js").init_lang_system, c = t("system.js").rpc, u = t("static-info.js"), d = t("navi.js").navi, p = t("notify-system.js").notify_sys, h = (t("nano-ajax.js").ajaxSend, 
        t("multilang.js").lang), _ = t("luna-global-checkers.js").global_checkers_init, f = t("luna-global-helpers.js").global_helpers_init, m = t("../../classic-menu/index.js"), v = t("../../basic-components/index.js"), b = t("./spa-components.js"), g = t("../../luna-quick-menu/index.js"), x = t("../../../lib/js/nbn-lib-components.js"), w = t("../../login/index.js"), y = t("../../notify/index.js"), j = t("../../firewall/index.js"), k = t("../../router/js/router.js").Router, N = t("../../router/js/routes.js").routes, I = t("../../cpe/js/cpe.js").cpe, E = t("data_utility.js").load_capabilities;
        function P(t) {
            var e = h();
            return p().error(e.error.error_apply + "(" + JSON.stringify(t) + ")"), !0;
        }
        function A(t) {
            return p().error(t), !0;
        }
        a.navi = d, a.lang = h, a.hub = new o(), 1 == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_LUNA && (v.registry(a.hub), m.registry(a.hub), w.registry(a.hub), y.registry(a.hub), 
        g.registry(a.hub)), x.registry(a.hub), b.registry(a.hub), j.registry(a.hub), a.RM = new s(a.hub), 
        1 == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_LUNA && (u.update(), a.menu = m.menu, a.info = {
            static: u
        }), 1 == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_LUNA && (u = (m = t("luna-twz.js")).twz, m = m.twz_init, a.twz = u, a.twz_init = m, 
        "RTC" == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_CUSTOMER && (m = t("../../luna-wizard/index.js").wizard_package, t = t("../../luna-wizard/lib/js/region_list.js").wizard_profiles, 
        a.wizard = m, a.wizard.profiles = t(), a.wizard())), i = Promise.all([ l(), E() ]), 
        document.addEventListener("DOMContentLoaded", function() {
            1 == {
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            }.CONFIG_LUNA && (_({
                notify: A
            }), f({})), i.then(function(t) {
                a.router = new k(N);
            }).then(function(t) {
                a.twz_init && a.twz_init(r.dom("app"), a.hub, a.RM);
            });
        }), 1 == {
            end: 0,
            CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
            CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
            BUILD: "debug"
        }.CONFIG_LUNA && (a.l2tp = {
            connect: function(t) {
                p().applying("connect"), console.log(t), c("l2tp_control_by_name", {
                    ifname: t,
                    op: "LC_UP"
                }).then(function() {
                    return p().done();
                }).catch(P);
            },
            disconnect: function(t) {
                p().applying("disconnect"), c("l2tp_control_by_name", {
                    ifname: t,
                    op: "LC_DOWN"
                }).then(function() {
                    return p().done();
                }).catch(P);
            }
        }), window.app = function() {
            return a;
        }, window.cpe = function() {
            return I;
        };
    }, {
        "../../../lib/js/nbn-lib-components.js": 17,
        "../../basic-components/index.js": 49,
        "../../classic-menu/index.js": 57,
        "../../cpe/js/cpe.js": 67,
        "../../firewall/index.js": 72,
        "../../login/index.js": 74,
        "../../luna-quick-menu/index.js": 76,
        "../../luna-wizard/index.js": 118,
        "../../luna-wizard/lib/js/region_list.js": 120,
        "../../notify/index.js": 147,
        "../../router/js/router.js": 150,
        "../../router/js/routes.js": 151,
        "./spa-components.js": 159,
        "data_utility.js": 4,
        "luna-global-checkers.js": 156,
        "luna-global-helpers.js": 157,
        "luna-twz.js": 113,
        "multilang.js": 15,
        "nano-ajax.js": 137,
        "navi.js": 16,
        "notify-system.js": 19,
        "static-info.js": 22,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    153: [ function(t, e, n) {
        "use strict";
        var s = r(t("nano-dom.js")), i = r(t("os.js")), o = r(t("nano-json-rpc-2.js"));
        function r(t) {
            return t && t.__esModule ? t : {
                default: t
            };
        }
        var a = t("../../../../autoconf.json");
        e.exports = {
            render: function(t) {
                var e = {
                    cols: [ "?????? N?N?N?N?????N?N????°", "???°?·???°?????µ N?N?N?N?????N?N????°", "???µN????? ?????????»N?N??µ????N?", "?????»?° N????????°?»?°", "??N??µ??N? ?????????»N?N??µ????N?", "IP ?°??N??µN?", "MAC ?°??N??µN?" ],
                    createNodeDOM: function() {
                        var t = s.default.table().setClass("rt_table"), e = this.cols, n = t.newHead();
                        return e.forEach(function(t) {
                            n.newHeadCell().add(t);
                        }), this.wlan = {
                            tbl_body: t.newBody(),
                            render: function(t) {
                                var o = s.default.table().newBody(), r = [];
                                t.forEach(function(t) {
                                    var e, n, i, s = (e = t.wlan_idx, n = t.virtual_idx, i = "WiFi ", i += 0 == e ? "2.4 ????N?" : "5 ????N?", 
                                    0 < n && (i += " (????N?N??µ???°N?)"), i);
                                    t.list.forEach(function(t) {
                                        var e = o.newRow();
                                        r.push(e), e.newCell().add(t.vendorClass || "-"), e.newCell().add(t.hostName || "-"), 
                                        e.newCell().add(s), e.newCell().add(t.rssi + " ??????"), e.newCell().add(t.link_time + " N??µ??"), 
                                        e.newCell().add(t.ip || "-"), e.newCell().add(t.mac);
                                    });
                                });
                                var e = s.default.el(this.tbl_body.e);
                                e.empty(), r.forEach(function(t) {
                                    return e.add(t);
                                });
                            }
                        }, this.lan = {
                            tbl_body: t.newBody(),
                            render: function(t) {
                                var n = s.default.table().newBody(), i = [];
                                t.forEach(function(t) {
                                    var e = n.newRow();
                                    i.push(e), e.newCell().add(t.vendorClass || "-"), e.newCell().add(t.hostName || "-"), 
                                    e.newCell().add("LAN" + t.port), e.newCell().add("-"), e.newCell().add(t.start ? t.start + " ???µ??" : "-"), 
                                    e.newCell().add(t.ip || "-"), e.newCell().add(t.mac);
                                });
                                var e = s.default.el(this.tbl_body.e);
                                e.empty(), i.forEach(function(t) {
                                    return e.add(t);
                                });
                            }
                        }, this.ctrl = t.e;
                    }
                };
                function n() {
                    (0, o.default)("wlan_clients_list", {}).then(function(t) {
                        return e.wlan.render(t);
                    }), (0, o.default)("lan_clients_list", {}).then(function(t) {
                        return e.lan.render(t);
                    });
                }
                s.default.dom(t).add(s.default.tag("blockquote").add(s.default.tag("h2").add("???»???µ??N?N?")).add(s.default.label("?­N??° N??°?±?»??N??° ??N????±N??°?¶?°?µN? ??N??µN? ?????????»N?N??µ????N?N? ???»???µ??N?????.")).add(s.default.tag("hr").attr("noshade", "").attr("size", 1).attr("top")).add(e.createNodeDOM())), 
                1 !== a.defines.CONFIG_LUNA && (document.head.getElementsByTagName("link")[0].href = "style.css"), 
                n(), i.default.poll(1e3, n);
            }
        };
    }, {
        "../../../../autoconf.json": 1,
        "nano-dom.js": 138,
        "nano-json-rpc-2.js": 139,
        "os.js": 146
    } ],
    154: [ function(t, e, n) {
        e.exports = {
            menu: {
                status: "Status",
                settings: "Settings",
                wlan0: "Wi-Fi 2.4G",
                wlan1: "Wi-Fi 5G",
                firewall: "Firewall",
                additional: "Additional",
                managment: "Managment",
                stats: "Statistics",
                clients: "Clients",
                routers: "Routers"
            }
        };
    }, {} ],
    155: [ function(t, e, n) {
        "use strict";
        var i, s = t("nbn-lib-components.js"), o = (i = s) && i.__esModule ? i : {
            default: i
        };
        t("polyfill.js");
        var r = (0, t("data_utility.js").load_capabilities)(), a = t("../../../../autoconf.json"), l = t("virtual-dom.js").RenderMachine, c = new (t("virtual-dom.js").ComponentHub)();
        o.default.registry(c), c.registry("ex-ip-filter", t("access-ex-ipFilter.vd").Ctor), 
        c.registry("ip-filter", t("access-ipFilter.vd").Ctor), c.registry("single-apmib-text", t("single-apmib-text.vd").Ctor), 
        c.registry("single-apmib-password", t("single-apmib-password.vd").Ctor), c.registry("single-apmib-select", t("single-apmib-select.vd").Ctor), 
        c.registry("proxy-form", t("proxy-form.vd").Ctor), c.registry("add-rule-form", t("add-rule-form.vd").Ctor), 
        c.registry("input-text-row", t("input-text-row.vd").Ctor), c.registry("input-select-row", t("input-select-row.vd").Ctor), 
        c.registry("input-range-row", t("input-range-row.vd").Ctor), c.registry("remove-list-form", t("remove-list-form.vd").Ctor), 
        c.registry("global-error-handler", t("global-error-handler.vd").Ctor), c.registry("exctractor", t("exctractor.vd").Ctor), 
        c.registry("ip-range", t("ip-range.vd").Ctor), c.registry("ip", t("ip.vd").Ctor), 
        c.registry("port-range", t("port-range.vd").Ctor), c.registry("port-range-split", t("port-range-split.vd").Ctor), 
        c.registry("select-protocol", t("select-protocol.vd").Ctor), c.registry("input-checkbox-row", t("input-checkbox-row.vd").Ctor), 
        c.registry("input-checkbox-checkbox-row", t("input-checkbox-checkbox-row.vd").Ctor), 
        c.registry("ros-ip-filter", t("ros-ip-filter.vd").Ctor), c.registry("abstract", t("abstract.vd").Ctor), 
        c.registry("single-apmib-checkbox", t("ros_single_apmib_checkbox.vd").Ctor), c.registry("ros-rm-list", t("ros-rm-list.vd").Ctor), 
        c.registry("ros-error-log", t("ros-error-log-popup.vd").Ctor), c.registry("add-mac-to-table-form", t("add_mac_to_table_form.vd").Ctor), 
        1 !== a.defines.CONFIG_LUNA ? document.head.getElementsByTagName("link")[0].href = "style.css" : console.log("styles should be style.css"), 
        e.exports = {
            render: function(n) {
                var i = new l(c);
                r.then(function(t) {
                    var e = i.render_component("ros-ip-filter");
                    i.mount(e, n);
                });
            }
        };
    }, {
        "../../../../autoconf.json": 1,
        "abstract.vd": 162,
        "access-ex-ipFilter.vd": 27,
        "access-ipFilter.vd": 28,
        "add-rule-form.vd": 29,
        "add_mac_to_table_form.vd": 163,
        "data_utility.js": 4,
        "exctractor.vd": 30,
        "global-error-handler.vd": 31,
        "input-checkbox-checkbox-row.vd": 34,
        "input-checkbox-row.vd": 35,
        "input-range-row.vd": 36,
        "input-select-row.vd": 37,
        "input-text-row.vd": 38,
        "ip-range.vd": 39,
        "ip.vd": 40,
        "nbn-lib-components.js": 17,
        "polyfill.js": 21,
        "port-range-split.vd": 41,
        "port-range.vd": 42,
        "proxy-form.vd": 43,
        "remove-list-form.vd": 44,
        "ros-error-log-popup.vd": 168,
        "ros-ip-filter.vd": 170,
        "ros-rm-list.vd": 172,
        "ros_single_apmib_checkbox.vd": 175,
        "select-protocol.vd": 45,
        "single-apmib-password.vd": 46,
        "single-apmib-select.vd": 47,
        "single-apmib-text.vd": 48,
        "virtual-dom.js": 26
    } ],
    156: [ function(t, e, n) {
        "use strict";
        var o = t("multilang.js").lang, r = alert;
        function i(t) {
            var e = o();
            return "" == t.value ? (r(e.W.LANG_INVALID_IPV4_ADDR_SHOULD_NOT_EMPTY), t.value = t.defaultValue, 
            t.focus(), !1) : 0 == validateKey(t.value) ? (r(e.W.LANG_INVALID_IPV4_ADDR_SHOULD_BE_DECIMAL_NUM), 
            t.value = t.defaultValue, t.focus(), !1) : 1 == IsLoopBackIP(t.value) ? (r(e.W.LANG_INVALID_IPV4_ADDR), 
            t.value = t.defaultValue, t.focus(), !1) : checkDigitRange(t.value, 1, 0, 255) ? checkDigitRange(t.value, 2, 0, 255) ? checkDigitRange(t.value, 3, 0, 255) ? !!checkDigitRange(t.value, 4, 1, 254) || (r(e.W.LANG_INVALID_IPV4_ADDR_4TH_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1) : (r(e.W.LANG_INVALID_IPV4_ADDR_3RD_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1) : (r(e.W.LANG_INVALID_IPV4_ADDR_2ND_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1) : (r(e.W.LANG_INVALID_IPV4_ADDR_1ST_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1);
        }
        function s(t, e) {
            var n = o();
            return 1 == e && "" == t.value ? (r(n.W.LANG_INVALID_IPV4_ADDR_SHOULD_NOT_EMPTY), 
            t.value = t.defaultValue, t.focus(), !1) : 0 == validateKey(t.value) ? (r(n.W.LANG_INVALID_IPV4_ADDR_SHOULD_BE_DECIMAL_NUM), 
            t.value = t.defaultValue, t.focus(), !1) : 1 == IsLoopBackIP(t.value) || 1 == IsInvalidIP(t.value) ? (r(n.W.LANG_INVALID_IPV4_ADDR), 
            t.value = t.defaultValue, t.focus(), !1) : checkDigitRange(t.value, 1, 1, 223) ? checkDigitRange(t.value, 2, 0, 255) ? checkDigitRange(t.value, 3, 0, 255) ? !!checkDigitRange(t.value, 4, 0, 254) || (r(n.W.LANG_INVALID_IPV4_ADDR_4TH_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1) : (r(n.W.LANG_INVALID_IPV4_ADDR_3RD_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1) : (r(n.W.LANG_INVALID_IPV4_ADDR_2ND_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1) : (r(n.W.LANG_INVALID_IPV4_ADDR_1ST_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1);
        }
        function a(t, e) {
            var n = o();
            return 1 == e && "" == t.value ? (r(n.W.LANG_INVALID_IPV4_ADDR_SHOULD_NOT_EMPTY), 
            t.value = t.defaultValue, t.focus(), !1) : 0 == validateKey(t.value) ? (r(n.W.LANG_INVALID_IPV4_ADDR_SHOULD_BE_DECIMAL_NUM), 
            t.value = t.defaultValue, t.focus(), !1) : 1 == IsLoopBackIP(t.value) || 1 == IsInvalidIP(t.value) ? (r(n.W.LANG_INVALID_IPV4_ADDR), 
            t.value = t.defaultValue, t.focus(), !1) : checkDigitRange(t.value, 1, 0, 255) ? checkDigitRange(t.value, 2, 0, 255) ? checkDigitRange(t.value, 3, 0, 255) ? !!checkDigitRange(t.value, 4, 0, 255) || (r(n.W.LANG_INVALID_IPV4_ADDR_4TH_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1) : (r(n.W.LANG_INVALID_IPV4_ADDR_3RD_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1) : (r(n.W.LANG_INVALID_IPV4_ADDR_2ND_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1) : (r(n.W.LANG_INVALID_IPV4_ADDR_1ST_DIGIT), 
            t.value = t.defaultValue, t.focus(), !1);
        }
        function l(t, e) {
            var n, i, s = o();
            if (1 == e && "" == t.value) return r(s.W.LANG_INVALID_IPV4_SUBNET_SHOULD_NOT_EMPTY), 
            t.value = t.defaultValue, t.focus(), !1;
            if (0 == validateKey(t.value)) return r(s.W.LANG_INVALID_IPV4_SUBNET_SHOULD_BE_DECIMAL_NUM), 
            t.value = t.defaultValue, t.focus(), !1;
            for (n = 1; n <= 4; n++) if (0 != (i = getDigit(t.value, n)) && 128 != i && 192 != i && 224 != i && 240 != i && 248 != i && 252 != i && 254 != i && 255 != i) return r(s.W.LANG_INVALID_IPV4_SUBNET_DIGIT), 
            t.focus(), !1;
            return !0;
        }
        function c(t, e) {
            var n, i = o(), s = 0;
            if (1 == e && 0 == t.value.length) return r(i.W.LANG_INVALID_MAC_ADDR_SHOULD_NOT_EMPTY), 
            !1;
            if (0 < t.value.length && t.value.length < 12) return r(i.W.LANG_INVALID_MAC_ADDR_NOT_COMPLETE), 
            t.focus(), !1;
            for (0 == t.value.length && (s = -1), n = 0; n < t.value.length; n++) "f" != t.value.charAt(n) && "F" != t.value.charAt(n) || s++;
            if (s == t.value.length || "000000000000" == t.value) return r(i.W.LANG_INVALID_MAC_ADDR), 
            t.focus(), !1;
            for (n = 0; n < t.value.length; n++) if (!("0" <= t.value.charAt(n) && t.value.charAt(n) <= "9" || "a" <= t.value.charAt(n) && t.value.charAt(n) <= "f" || "A" <= t.value.charAt(n) && t.value.charAt(n) <= "F")) return r(i.W.LANG_INVALID_MAC_ADDR_SHOULD_BE), 
            t.focus(), !1;
            return !0;
        }
        e.exports.global_checkers_init = function(t) {
            t && t.notify && (r = t.notify), window.checkIP = i, window.checkHostIP = s, window.checkMac = c, 
            window.checkNetmask = l, window.checkNetIP = a;
        };
    }, {
        "multilang.js": 15
    } ],
    157: [ function(t, e, n) {
        "use strict";
        var i = t("system.js").$;
        function s(t, e) {
            t = i.dom(t);
            t && t.show(e);
        }
        function o(t, e) {
            Array.isArray(t) ? t.forEach(function(t) {
                return s(t, e);
            }) : s(t, e);
        }
        e.exports.global_helpers_init = function(t) {
            window.showById = o;
        };
    }, {
        "system.js": 23
    } ],
    158: [ function(t, e, n) {
        e.exports = {
            menu: {
                status: "??N??°N?N?N?",
                settings: "???°N?N?N????????°",
                wlan0: "Wi-Fi 2.4G",
                wlan1: "Wi-Fi 5G",
                firewall: "???µ?¶N??µN??µ?????? N???N??°??",
                additional: "?????????»????N??µ?»N?????",
                managment: "????N??°???»?µ?????µ",
                stats: "CN??°N???N?N??????°",
                clients: "???»???µ??N?N?",
                routers: "???°N?N?N?N?N?N?",
                wan: "WAN",
                lan: "LAN",
                wlmultipleap: "????N?N??µ??N??µ N??µN???",
                wlsecurity: "???µ?·?????°N?????N?N?N?",
                wlactrl: "??????N?N????»N? ????N?N?N????°",
                wds: "WDS",
                wdssecurity: "WDS ???µ?·?????°N?????N?N?N?",
                wdslist: "WDS ??????N?????",
                radar: "Wi-Fi N??°???°N?",
                wps: "WPS",
                wlschedule: "? ?°N?????N??°?????µ",
                wlbasic: "??N?????????N??µ",
                wladvanced: "?????????»????N??µ?»N?????",
                portforwarding: "??N????±N???N? ????N?N?????",
                portfilter: "?¤???»N?N?N? ????N?N?????",
                ipfilter: "?¤???»N?N?N? IP",
                macfilter: "?¤???»N?N?N? MAC",
                whitefilter: "???µ?»N??? N?????N?????",
                urlfilter: "?¤???»N?N?N? URL",
                dos: "???°N???N??° ??N? DOS",
                dmz: "DMZ",
                alg: "ALG",
                accessctl: "??????N?N????»N? ????N?N?N????°",
                ddns: "DDNS",
                igmp: "IGMP",
                accounts: "??N??µN???N??µ ?·?°????N???",
                route: "???°N?N?N?N?N????·?°N???N?",
                qos: "QoS",
                qospolitics: "QoS ?????»??N???????",
                qosclass: "QoS ???»?°N?N???N??????°N???N?",
                config: "??????N?????N?N??°N???N?",
                updatefw: "???±???????»?µ?????µ ????",
                ntp: "??N??µ??N?",
                log: "??N?N????°?»",
                tr069: "TR-069",
                reboot: "???µN??µ?·?°??N?N??·???°",
                exit: "??N?N?????"
            },
            macFilter: {
                black_title: "MAC ?¤???»N?N?N?",
                black_enable_text: "?????»N?N???N?N? N??µN???N??? N?????N?????",
                black_description: "???°????N??? ?? N?N????? N??°?±?»??N??µ ??N??????»N??·N?N?N?N?N? ???»N? ????N??°????N??µ????N? N????????? ???°???µN????? ???°????N?N?, ???µN??µ???°???°?µ??N?N? ???· ???°N??µ?? ?»?????°?»N??????? N??µN??? ?? ?˜??N??µN????µN? N??µN??µ?· N???N?N??µN?. ?˜N??????»N??·?????°?????µ N?N???N? N????»N?N?N????? ?????¶?µN? ??????N?N???N?N? ?±?µ?·?????°N?????N?N?N? ???»?? ????N??°????N???N?N? ????N?N?N??? ?? ????N??µN????µN? ???· ???°N??µ?? ?»?????°?»N??????? N??µN???.",
                mac_address: "MAC-?°??N??µN?:",
                mac_comment: "?????????µ??N??°N?????:",
                mac_add_dev: "?????±?°????N?N? N?N?N?N?????N?N?????"
            },
            udpxy: {
                title: "Udpxy",
                description: "CN?N??°????N??° ???»N? ??????N?????N?N??°N????? Udpxy.",
                enable: "?????»N?N???N?N? UDPXY",
                buffer: "??N?N??µN?",
                proxy_ip: "IP ?°??N??µN? ??N?????N???",
                proxy_port: "????N?N? ??N?????N???",
                timeout: "???°????-?°N?N?"
            },
            buttons: {
                save: "????N?N??°????N?N? ?? ??N??????µ????N?N?"
            },
            notify: {
                send: "??N???N??°?????° ???°????N?N?",
                done: "????N???????"
            }
        };
    }, {} ],
    159: [ function(e, t, n) {
        "use strict";
        t.exports.registry = function(t) {
            t.registry("ex-ip-filter", e("access-ex-ipFilter.vd").Ctor), t.registry("ip-filter", e("access-ipFilter.vd").Ctor), 
            t.registry("single-apmib-text", e("single-apmib-text.vd").Ctor), t.registry("single-apmib-password", e("single-apmib-password.vd").Ctor), 
            t.registry("single-apmib-select", e("single-apmib-select.vd").Ctor), t.registry("proxy-form", e("proxy-form.vd").Ctor), 
            t.registry("add-rule-form", e("add-rule-form.vd").Ctor), t.registry("input-text-row", e("ros-input-text.vd").Ctor), 
            t.registry("input-select-row", e("input-select-row.vd").Ctor), t.registry("input-range-row", e("input-range-row.vd").Ctor), 
            t.registry("remove-list-form", e("remove-list-form.vd").Ctor), t.registry("global-error-handler", e("global-error-handler.vd").Ctor), 
            t.registry("exctractor", e("exctractor.vd").Ctor), t.registry("ip-range", e("ip-range.vd").Ctor), 
            t.registry("ip", e("ip.vd").Ctor), t.registry("row-info", e("ros-row-info.vd").Ctor), 
            t.registry("port-range", e("port-range.vd").Ctor), t.registry("port-range-split", e("port-range-split.vd").Ctor), 
            t.registry("select-protocol", e("select-protocol.vd").Ctor), t.registry("input-checkbox-row", e("input-checkbox-row.vd").Ctor), 
            t.registry("input-checkbox-checkbox-row", e("input-checkbox-checkbox-row.vd").Ctor), 
            t.registry("mac-filter-template", e("ros-mac-filter.vd").Ctor), t.registry("mac-white-list", e("mac-white-list.vd").Ctor), 
            t.registry("mac-black-list", e("mac-black-list.vd").Ctor), t.registry("url-filter", e("url-filter.vd").Ctor), 
            t.registry("unity-status", e("unity-status.vd").Ctor), t.registry("udpxy", e("udpxy.vd").Ctor), 
            t.registry("submit-notify", e("submit-notify.vd").Ctor), t.registry("abstract", e("abstract.vd").Ctor), 
            t.registry("single-apmib-checkbox", e("ros_single_apmib_checkbox.vd").Ctor), t.registry("add-mac-to-table-form", e("add_mac_to_table_form.vd").Ctor), 
            t.registry("ros-rm-list", e("ros-rm-list.vd").Ctor), t.registry("ros-error-log", e("ros-error-log-popup.vd").Ctor), 
            t.registry("change-password", e("password.vd").Ctor), t.registry("ros-select", e("ros-select-row.vd").Ctor), 
            t.registry("ros-checkbox-row", e("ros-checkbox-row.vd").Ctor);
        };
    }, {
        "abstract.vd": 162,
        "access-ex-ipFilter.vd": 27,
        "access-ipFilter.vd": 28,
        "add-rule-form.vd": 29,
        "add_mac_to_table_form.vd": 163,
        "exctractor.vd": 30,
        "global-error-handler.vd": 31,
        "input-checkbox-checkbox-row.vd": 34,
        "input-checkbox-row.vd": 35,
        "input-range-row.vd": 36,
        "input-select-row.vd": 37,
        "ip-range.vd": 39,
        "ip.vd": 40,
        "mac-black-list.vd": 164,
        "mac-white-list.vd": 165,
        "password.vd": 166,
        "port-range-split.vd": 41,
        "port-range.vd": 42,
        "proxy-form.vd": 43,
        "remove-list-form.vd": 44,
        "ros-checkbox-row.vd": 167,
        "ros-error-log-popup.vd": 168,
        "ros-input-text.vd": 169,
        "ros-mac-filter.vd": 171,
        "ros-rm-list.vd": 172,
        "ros-row-info.vd": 173,
        "ros-select-row.vd": 174,
        "ros_single_apmib_checkbox.vd": 175,
        "select-protocol.vd": 45,
        "single-apmib-password.vd": 46,
        "single-apmib-select.vd": 47,
        "single-apmib-text.vd": 48,
        "submit-notify.vd": 176,
        "udpxy.vd": 177,
        "unity-status.vd": 178,
        "url-filter.vd": 179
    } ],
    160: [ function(t, e, n) {
        "use strict";
        var i = t("./rus.json");
        t("./en.json");
        n.lang = function() {
            return "MTS" == {
                end: 0,
                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                BUILD: "debug"
            }.CONFIG_CUSTOMER && (i.macFilter.black_title = "?§?µN???N??? N?????N?????"), i;
        };
    }, {
        "./en.json": 154,
        "./rus.json": 158
    } ],
    161: [ function(t, e, n) {
        "use strict";
        var i = t("../../../lib/js/clone-mac.js").generate_clone_mac_simple, s = t("../../../lib/js/data_utility.js").wlanClientList, o = t("../../../lib/js/system.js").$, r = t("./spa-lang.js").lang;
        e.exports.render = function() {
            var t = r();
            i("wlanMacListWidget", t.macFilter.mac_add_dev, s, function(t, e) {
                o.dom("mac-address").value(t), o.dom("mac-comment").value(e);
            });
        };
    }, {
        "../../../lib/js/clone-mac.js": 3,
        "../../../lib/js/data_utility.js": 4,
        "../../../lib/js/system.js": 23,
        "./spa-lang.js": 160
    } ],
    162: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            e = e.attr.text || "";
            this.tree = new i("div", {}), this.tree.root().set_class("abstract").child("label", {
                text: e
            }).up().child("hr", {
                size: "1",
                noshade: "",
                align: "top"
            }).up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    163: [ function(p, t, e) {
        "use strict";
        var h = p("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = (0, p("spa-lang.js").lang)(), i = p("form-widgets.js").AddRuleFormInputs, s = p("nano-json-rpc-2.js"), o = (p("dom-maker.js").macs_2_table, 
            p("error-handler.js")), r = (o.Syslog, o.LOG, p("validations.js").chain_valid, p("validations.js").state_to_str, 
            {}), a = {}, l = this.exports = {}, c = e.attr || {}, u = c.method_add || "", d = {};
            this.obj = {
                mounted: function() {
                    l.clear = function() {
                        r.exports.set_value(""), a.exports.set_value("");
                    }, d = l.form = new i([ r.el, a.el ], function() {
                        return s(u, {
                            mac: r.exports.get_value(),
                            comment: a.exports.get_value()
                        });
                    });
                }
            }, this.tree = new h("div", {}), this.tree.root().child("clone-mac-lan-wlan-simple", {
                setter: function(t, e) {
                    r.exports.set_value(t), a.exports.set_value(e), c.submit && c.submit.el.disabled(!1), 
                    d.change();
                },
                text: n.macFilter.mac_add_dev
            }).bind(a).directive("bind", a).up().child("input-text-row", {
                text: n.macFilter.mac_address,
                id: "mac-address"
            }).bind(r).directive("bind", r).up().child("input-text-row", {
                text: n.macFilter.mac_comment,
                id: "mac-comment"
            }).bind(a).directive("bind", a).up();
        };
    }, {
        "dom-maker.js": 5,
        "error-handler.js": 7,
        "form-widgets.js": 9,
        "nano-json-rpc-2.js": 139,
        "spa-lang.js": 160,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    164: [ function(s, t, e) {
        "use strict";
        var o = s("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = (0, s("spa-lang.js").lang)(), i = document.head.getElementsByTagName("link");
            this.obj = {
                created: function() {
                    i[0].href = "style.css";
                }
            }, this.tree = new o("mac-filter-template", {
                title: n.macFilter.black_title,
                enable_text: n.macFilter.black_enable_text,
                text: n.macFilter.black_description,
                mib_enabled: "macBlack",
                mib_table_name: "MacFilterlist"
            }), this.tree.root();
        };
    }, {
        "spa-lang.js": 160,
        "virtual-dom.js": 26
    } ],
    165: [ function(t, e, n) {
        "use strict";
        var i = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            var n = document.head.getElementsByTagName("link");
            this.obj = {
                created: function() {
                    n[0].href = "style.css";
                }
            }, this.tree = new i("mac-filter-template", {
                title: "???µ?»N??? N?????N?????",
                enable_text: "?????»N?N???N?N? ?±?µ?»N??? N?????N?????",
                mib_enabled: "macWhite",
                mib_table_name: "MacWhitelist"
            }), this.tree.root().text("\n\t\t???°????N??? ?? N?N????? N??°?±?»??N??µ ??N??????»N??·N?N?N?N?N? ???»N? ????N??°????N??µ????N? N????????? ???°???µN????? ???°????N?N?, ???µN??µ???°???°?µ??N?N? ???· ???°N??µ?? ?»?????°?»N??????? N??µN??? ?? ?˜??N??µN????µN? N??µN??µ?· N???N?N??µN?. ?˜N??????»N??·?????°?????µ N?N???N? N????»N?N?N????? ?????¶?µN? ??????N?N???N?N? ?±?µ?·?????°N?????N?N?N? ???»?? ????N??°????N???N?N? ????N?N?N??? ?? ????N??µN????µN? ???· ???°N??µ?? ?»?????°?»N??????? N??µN???.\n\t");
        };
    }, {
        "virtual-dom.js": 26
    } ],
    166: [ function(m, t, e) {
        "use strict";
        var v = m("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var s = m("system.js").rpc, n = m("system.js").makePending, i = m("dom-maker.js"), o = (i.ex_ip_2_table, 
            i.opts_act, m("error-handler.js")), i = o.Syslog, r = o.LOG, i = (m("validations.js").chain_valid, 
            m("validations.js").state_to_str, new i("ExIpFilter", {
                level: r.DEBUG
            }), (0, m("data_utility.js").get_capabilities)().isSuper ? [ {
                value: "admin",
                text: "admin"
            }, {
                value: "superadmin",
                text: "superadmin"
            } ] : [ {
                value: "admin",
                text: "admin"
            } ]), a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = document.head.getElementsByTagName("link");
            var _ = n(a);
            var f = this.exports = {};
            this.obj = {
                created: function() {
                    h[0].href = "style.css", l.el.show(!1), p.el.disabled(!0), f.pass = u.el;
                    var e = !0, n = !0;
                    function i() {
                        !function() {
                            var t = u.exports.get_value();
                            if (0 == t.length) return u.exports.invalid("??N?N?N????? ???°N????»N?"), e = !1;
                            if (/\s/.test(t)) return u.exports.invalid("???????????» ??N????±?µ?»?° ???µ ??????N?N?N?????"), e = !1;
                            if ("MTS" != {
                                end: 0,
                                CONFIG_LUNA_WEB_MULTIWAN_PPPIPSETTINGS: "web",
                                CONFIG_LUNA_WEB_MULTIWAN_CHANNEL_MODE: "web",
                                BUILD: "debug"
                            }.CONFIG_CUSTOMER) {
                                if (t.length < 8) return u.exports.invalid("????N???N??????? ???°N????»N?"), e = !1;
                                if (!/\d/.test(t)) return u.exports.invalid("???µ N??????µN??¶??N? N???N?N? ?±N? ??????N? N???N?N?N?"), e = !1;
                                if (!/[a-z]/.test(t)) return u.exports.invalid("???µ N??????µN??¶??N? N???N?N? ?±N? ???????? N?N?N???N???N??? N??????????»"), 
                                e = !1;
                                if (!/[A-Z]/.test(t)) return u.exports.invalid("???µ N??????µN??¶??N? N???N?N? ?±N? ???????? ??N???????N??????? N??????????»"), 
                                e = !1;
                                if (/[^0-9a-zA-Z]/.test(t)) return u.exports.invalid("?????µN?N??????????»N? ???µ ??????N?N?N?????N?"), 
                                e = !1;
                            }
                            e = !0, u.exports.valid();
                        }(), function() {
                            if (d.exports.get_value() != u.exports.get_value()) return d.exports.invalid("???°N????»?? ???µ N????????°???°N?N?"), 
                            n = !1;
                            n = !0, d.exports.valid();
                        }(), p.el.disabled(!e || !n);
                    }
                    u.el.on("input", function(t) {
                        i();
                    }), d.el.on("input", function(t) {
                        i();
                    }), p.el.on("click", function(t) {
                        e && n && (_.run(), s("change_password", {
                            user: c.exports.get_value(),
                            pass: u.exports.get_value()
                        }).then(function(t) {
                            _.good(), d.exports.set_value(""), u.exports.set_value(""), p.el.disabled(!0);
                        }).catch(function(t) {
                            p.el.disabled(!0), console.log("err", t), _.good();
                        }));
                    });
                },
                mounted: function() {}
            }, this.tree = new v("blockquote", {}), this.tree.root().child("h2", {}).text("??N??µN???N??µ ?·?°????N???").up().child("abstract", {}).text("??N?N??°????N??° ??N??????»N??·N??µN?N?N? ???»N? N???N??°???»?µ????N? N?N??µN???N????? ?·?°????N?N????? ???»N? ????N?N?N????° ?? Web ????N??µN?N??µ??N?N? N???N?N??µN??°.").up().text("\n \t\t").child("ros-error-log", {}).bind(a).directive("bind", a).up().text("\n \t\t").child("form", {
                autocomplete: "off",
                action: ""
            }).text("\n \t\t\t").child("input", {
                autocomplete: "false",
                name: "hidden",
                type: "text"
            }).bind(l).directive("bind", l).up().child("ros-select", {
                opts: i,
                name: "user"
            }).bind(c).directive("bind", c).text("?????»N??·?????°N??µ?»N?:").up().child("input-text-row", {
                type: "password",
                name: "pass"
            }).bind(u).directive("bind", u).text("??????N??? ???°N????»N?:").up().child("input-text-row", {
                type: "password",
                name: "duble_pass"
            }).bind(d).directive("bind", d).text("??????N????µN?????N?N? ??????N??? ???°N????»N?:").up().up().child("submit", {}).bind(p).directive("bind", p).text("????N?N??°????N?N? ?? ??N??????µ????N?N?").up();
        };
    }, {
        "data_utility.js": 4,
        "dom-maker.js": 5,
        "error-handler.js": 7,
        "system.js": 23,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    167: [ function(l, t, e) {
        "use strict";
        var c = l("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = l("event-emitter.js").EventEmiter, i = {}, e = e.attr.text || "", s = !1;
            function o() {}
            var r = new n(), a = this.exports = {};
            this.obj = {
                created: function() {
                    a.on = function(t, e) {
                        return r.on(t, e);
                    }, a.checkbox = i.el, a.input = i.el, a.valid = o, a.invalid = o, a.is_valid = function() {
                        return !0;
                    }, a.is_changed = function() {
                        return s;
                    }, a.changed = function() {
                        s = !0, r.emit("change", i.el.e.checked);
                    }, a.no_changed = function() {
                        s = !1;
                    }, a.get_value = function() {
                        return i.el.e.checked;
                    }, a.set_value = function(t) {
                        i.el.e.checked = t;
                    }, a.disabled = function(t) {
                        i.el.disabled(t);
                    }, i.el.on("change", function(t) {
                        s = !0, r.emit("change", i.el.e.checked);
                    });
                }
            }, this.tree = new c("div", {}), this.tree.root().set_class("row single-checkbox").child("checkbox", {}).bind(i).directive("bind", i).up().child("label", {
                text: e
            }).up();
        };
    }, {
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    168: [ function(u, t, e) {
        "use strict";
        var d = u("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var i = u("os.js"), n = {}, s = {}, o = {}, r = {}, a = {}, l = this.exports = {};
            function c(t, e) {
                var n = this;
                this.count = 1, t.set(e + this.get_dots()), this.poll = i.poll(500, function() {
                    t.set(e + n.get_dots());
                });
            }
            c.prototype.get_dots = function() {
                return this.count++, 3 < this.count && (this.count = 0), "...".slice(0, this.count);
            }, c.prototype.stop = function() {
                this.poll.cancel();
            }, this.obj = {
                created: function() {
                    n.el.show(!1), s.el.on("click", function() {
                        return n.el.show(!1);
                    }), l.error = function(t) {
                        s.el.show(!0), o.el.show(!0), n.el.show(!0), a.el.set(""), r.el.set(""), o.el.set(t);
                    }, l.good = function(t) {
                        s.el.show(!1), n.el.show(!0), a.el.set(""), o.el.set(""), r.el.set(t);
                    }, l.status_pending = function(t) {
                        return s.el.show(!1), n.el.show(!0), r.el.set(""), o.el.set(""), o.el.show(!1), 
                        a.el.set(t), new c(a.el, t);
                    }, l.clear = function() {
                        s.el.show(!1), n.el.show(!1), o.el.show(!1), a.el.set(""), r.el.set(""), o.el.set("");
                    };
                }
            }, this.tree = new d("div", {}), this.tree.root().set_class("submit-popup-wrapper").bind(n).directive("bind", n).child("div", {}).set_class("submit-popup").child("div", {}).set_class("error").child("label", {}).bind(o).directive("bind", o).up().up().child("div", {}).set_class("good").child("label", {}).bind(r).directive("bind", r).up().up().child("div", {}).set_class("status").child("label", {}).bind(a).directive("bind", a).up().up().child("div", {}).set_class("ok").child("button", {}).bind(s).directive("bind", s).text("Ok").up().up().up();
        };
    }, {
        "os.js": 146,
        "virtual-dom.js": 26
    } ],
    169: [ function(p, t, e) {
        "use strict";
        var h = p("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = p("event-emitter.js").EventEmiter, i = e.attr, s = i.text || "", o = i.name || "", e = i.type || "text", i = i.id || "", r = {}, a = {};
            function l(t, e) {
                this.input = t, this.name = e;
            }
            var c = !(l.prototype.extract = function(t) {
                return t[this.name] = this.input.get_value(), !0;
            }), u = new n(), d = this.exports = {};
            this.obj = {
                created: function() {
                    d.on = function(t, e) {
                        return u.on(t, e);
                    }, d.input = r, d.get_value = function() {
                        return r.el.e.value;
                    }, d.is_valid = function() {
                        return !0;
                    }, d.valid = function() {
                        r.el.setClass(""), a.el.set("");
                    }, d.invalid = function(t) {
                        r.el.setClass("invalid"), a.el.set(t);
                    }, d.set_value = function(t) {
                        r.el.e.value = t;
                    }, d.get_exctractor = function() {
                        return new l(d, o);
                    }, d.is_changed = function() {
                        return c;
                    }, d.changed = function() {
                        c = !0, u.emit("change", r.el.e.value);
                    }, d.no_changed = function() {
                        c = !1;
                    }, r.el.on("input", function(t) {
                        c = !0, u.emit("change", r.el.e.value);
                    });
                }
            }, this.tree = new h("div", {}), this.tree.root().child("div", {}).set_class("row").child("label", {
                text: s
            }).set_class("description").up().child("div", {}).set_class("input").child("input", {
                type: e,
                name: o,
                id: i
            }).set_class("invalid").bind(r).directive("bind", r).up().up().child("label", {}).set_class("input-error-desc").bind(a).directive("bind", a).up().up();
        };
    }, {
        "event-emitter.js": 8,
        "virtual-dom.js": 26
    } ],
    170: [ function(I, t, e) {
        "use strict";
        var E = I("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            I("nano-json-rpc-2.js");
            var n = I("dom-maker.js"), i = n.ex_ip_2_table, s = n.opts_act, o = I("error-handler.js"), r = o.Syslog, a = o.LOG, l = (I("validations.js").chain_valid, 
            I("validations.js").state_to_str), c = new r("ExIpFilter", {
                level: a.DEBUG
            }), u = {}, d = {}, p = {}, h = {}, _ = {}, f = {}, m = {}, v = {}, b = {}, g = {}, x = {}, w = {}, y = {}, j = [ m, y, v, b, g, x, w ], k = [ b, g, x, w ], n = {};
            o = {
                extract: function(t, e) {
                    var n = k.filter(function(t) {
                        return "" != t.exports.input.el.e.value;
                    });
                    return e.data(n, {
                        code: "post_exctract_no_empty"
                    }).valid("no_empty", {
                        required: !0
                    });
                }
            }, r = {
                run: function() {
                    this.pending = u.exports.status_pending("??N???N??°?????° ???°????N?N?");
                },
                stop: function() {
                    this.pending.stop();
                }
            };
            function N() {
                u.exports.good("????N???????"), setTimeout(function() {
                    u.exports.clear();
                }, 2e3);
            }
            this.obj = {
                mounted: function() {
                    d.exports.checkbox.on("click", function() {
                        h.el.show(d.exports.checkbox.e.checked);
                    });
                }
            }, this.tree = new E("blockquote", {}), this.tree.root().child("h2", {}).text("IP N????»N?N?N?").up().child("abstract", {}).text(" ???°????N??? ?? N?N????? N??°?±?»??N??µ ??N??????»N??·N?N?N?N?N? ???»N? ????N??°????N??µ????N? N????????? ???°???µN????? ???°????N?N?, ???µN??µ???°???°?µ??N?N? ???· ???°N??µ?? ?»?????°?»N???????\n N??µN??? ?? ?˜??N??µN????µN? N??µN??µ?· N???N?N??µN?. ?˜N??????»N??·?????°?????µ N?N???N? N????»N?N?N????? ?????¶?µN? ??????N?N???N?N? ?±?µ?·?????°N?????N?N?N? ???»?? ????N??°????N???N?N? \n ????N?N?N??? ?? ????N??µN????µN? ???· ???°N??µ?? ?»?????°?»N??????? N??µN???.").up().text("\n \t\t").child("ros-error-log", {}).bind(u).directive("bind", u).up().child("proxy-form", {
                error_handler: function(t) {
                    c.log(a.INFO, t), t && t.code && alert("??N????±???° ??N??????µ???µ????N? N???N???N?");
                },
                submit: f,
                pending: r,
                after_good_submit: N,
                sub_forms: function() {
                    return [ p, d, h ];
                },
                after_update: function() {
                    v.exports.input.el.value(3), m.exports.input.el.value(0), h.el.show(d.exports.checkbox.e.checked), 
                    _.exports.update();
                }
            }).bind(n).directive("bind", n).child("single-apmib-checkbox", {
                mib: "SPI"
            }).bind(p).directive("bind", p).text("SPI").up().child("single-apmib-checkbox", {
                mib: "ipFilterEnabled"
            }).bind(d).directive("bind", d).text("?????»N?N???N?N? IP ?¤???»N?N?N?").up().child("exctractor", {
                validator_res_handler: function(t) {
                    if (t.good()) return u.exports.clear(), !0;
                    var e = t.get_state();
                    return console.log([ "errors_log", e ]), e.msg.code && "post_exctract_no_empty" == e.msg.code ? u.exports.error("???? ???????? ?·???°N??µ?????µ ???µ ?·?°???°????.") : (t = e.msg.endsWith(":") ? e.msg.slice(0, -1) : e.msg, 
                    u.exports.error(t + " " + l(e.state))), !1;
                },
                inputs: j,
                post_exctract: o,
                method: "ipFilterlist__add"
            }).bind(h).directive("bind", h).child("input-select-row", {
                opts: s,
                name: "action"
            }).bind(m).directive("bind", m).text("??????:").up().child("select-protocol", {}).bind(v).directive("bind", v).up().child("ip-range", {
                names: [ "sourceFirstIp", "sourceLastIp", "sourceIpMask" ]
            }).bind(b).directive("bind", b).text("IP ?°??N??µN? (???»?? ?????°???°?·????) ??N?N???N????????°:").up().child("port-range", {
                names: [ "sourceFirstPort", "sourceLastPort" ]
            }).bind(g).directive("bind", g).text("????N?N? (???»?? ?????°???°?·????) ??N?N???N????????°:").up().child("ip-range", {
                names: [ "destFirstIp", "destLastIp", "destIpMask" ]
            }).bind(x).directive("bind", x).text("IP ?°??N??µN? (???»?? ?????°???°?·????) ???°?·???°N??µ????N?:").up().child("port-range", {
                names: [ "destFirstPort", "destLastPort" ]
            }).bind(w).directive("bind", w).text("????N?N? (???»?? ?????°???°?·????) ???°?·???°N??µ????N?:").up().child("input-text-row", {
                name: "comment"
            }).bind(y).directive("bind", y).text("?????????µ??N??°N?????:").up().up().child("submit", {}).bind(f).directive("bind", f).text("????N?N??°????N?N? ?? ??N??????µ????N?N?").up().up().child("ros-rm-list", {
                pending: r,
                after_good_submit: N,
                get_list: i,
                table_name: "ipFilterlist"
            }).bind(_).directive("bind", _).up();
        };
    }, {
        "dom-maker.js": 5,
        "error-handler.js": 7,
        "nano-json-rpc-2.js": 139,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    171: [ function(b, t, e) {
        "use strict";
        var g = b("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            b("nano-json-rpc-2.js");
            var n = b("dom-maker.js").macs_2_table, i = b("error-handler.js"), s = i.Syslog, o = i.LOG, r = (b("validations.js").chain_valid, 
            b("validations.js").state_to_str, new s("mac-filter", {
                level: o.DEBUG
            })), a = e.attr, l = a.text || "", c = a.title || "", u = a.mib_table_name || "", d = a.enable_text || "", p = a.mib_enabled || "", i = u + "__add", h = {}, _ = {}, f = {}, m = {}, s = {}, e = {};
            a = {
                run: function() {
                    this.pending = h.exports.status_pending("??N???N??°?????° ???°????N?N?");
                },
                stop: function() {
                    this.pending.stop();
                }
            };
            function v() {
                h.exports.good("????N???????"), setTimeout(function() {
                    h.exports.clear();
                }, 2e3), f.exports.clear();
            }
            this.obj = {
                mounted: function() {
                    _.exports.checkbox.on("click", function() {
                        f.el.show(_.exports.checkbox.e.checked);
                    });
                }
            }, this.tree = new g("blockquote", {}), this.tree.root().child("h2", {
                text: c
            }).up().child("abstract", {
                text: l
            }).up().text("\n \t\t").child("ros-error-log", {}).bind(h).directive("bind", h).up().child("proxy-form", {
                error_handler: function(t) {
                    r.log(o.INFO, t), t && t.code && alert("??N????±???° ??N??????µ???µ????N? N???N???N?");
                },
                submit: s,
                pending: a,
                after_good_submit: v,
                sub_forms: function() {
                    return [ _, f ];
                },
                after_update: function() {
                    f.el.show(_.exports.checkbox.e.checked), m.exports.update();
                }
            }).bind(e).directive("bind", e).child("single-apmib-checkbox", {
                mib: p,
                text: d
            }).bind(_).directive("bind", _).up().child("div", {}).set_class("add-rule").child("add-mac-to-table-form", {
                method_add: i,
                submit: s
            }).bind(f).directive("bind", f).up().up().child("submit", {}).bind(s).directive("bind", s).text("????N?N??°????N?N? ?? ??N??????µ????N?N?").up().up().child("ros-rm-list", {
                pending: a,
                after_good_submit: v,
                table_name: u,
                get_list: n
            }).bind(m).directive("bind", m).up();
        };
    }, {
        "dom-maker.js": 5,
        "error-handler.js": 7,
        "nano-json-rpc-2.js": 139,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    172: [ function(_, t, e) {
        "use strict";
        var f = _("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = _("dom-maker.js").render_rm_list_rostelecom, i = (_("nano-json-rpc-2.js"), 
            _("error-handler.js")), s = i.Syslog, o = i.LOG, r = (_("validations.js").chain_valid, 
            new s("ros-rm-list", {
                level: o.ERROR
            })), a = e.attr, l = (a.text, a.opts, a.after_good_submit), i = a.pending, s = a.table_name || "";
            function c() {}
            var o = a.get_list || c, u = this.exports = {}, e = a.error_handler || c, d = {}, p = {}, a = {}, h = {};
            this.obj = {
                mounted: function() {
                    u.update = h.exports.update, d.exports.form.set_syslog(r);
                }
            }, this.tree = new f("div", {}), this.tree.root().set_class("ros-table-rm-list").text(" \n\t\t").child("proxy-form", {
                pending: i,
                after_good_submit: l,
                error_handler: e,
                submit: p,
                sub_forms: function() {
                    return [ d ];
                },
                after_update: function() {
                    d.exports.form.onChange(function() {
                        return p.el.e.disabled = !1;
                    }), d.exports.form.onChange(function() {
                        return p.el.e.disabled = !1;
                    });
                }
            }).bind(h).directive("bind", h).child("remove-list-form", {
                table_name: s,
                get_list: o,
                list_maker: n
            }).bind(d).directive("bind", d).up().child("submit", {}).bind(p).directive("bind", p).text("?????°?»??N?N? ??N??±N??°???????µ").up().text("z\n\t\t\t").child("input", {
                type: "reset",
                value: "???±N???N???N?N?"
            }).bind(a).directive("bind", a).up().text("???±N???N???N?N?").up();
        };
    }, {
        "dom-maker.js": 5,
        "error-handler.js": 7,
        "nano-json-rpc-2.js": 139,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    173: [ function(t, e, n) {
        "use strict";
        var r = t("virtual-dom.js").VirtualDom;
        e.exports.Ctor = function(t, e) {
            var n = {}, i = e.attr, e = i.text || "", s = i.default || "", o = this.exports = {};
            this.obj = {
                created: function() {
                    n.el.set(s), o.set_value = function(t) {
                        n.el.set(t);
                    }, o.to_default = function(t) {
                        n.el.set(s);
                    };
                }
            }, this.tree = new r("div", {}), this.tree.root().set_class("row-info single-info").child("span", {}).set_class("single-info-name").child("label", {
                text: e
            }).up().up().child("span", {}).set_class("single-info-value").child("label", {}).bind(n).directive("bind", n).up().up();
        };
    }, {
        "virtual-dom.js": 26
    } ],
    174: [ function(l, t, e) {
        "use strict";
        var c = l("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            l("nano-dom.js");
            var n = e.attr, e = n.text || "", i = n.opts || [], s = n.name || "", o = this.exports = {}, r = {};
            function a(t, e) {
                this.input = t, this.name = e;
            }
            a.prototype.extract = function(t) {
                return t[this.name] = parseInt(this.input.get_value(), 10), !0;
            }, this.obj = {
                created: function() {
                    (o.input = r).el.addOptions(i), o.get_value = function() {
                        return r.el.e.value;
                    }, o.set_value = function(t) {
                        r.el.e.value = t;
                    }, o.get_exctractor = function() {
                        return new a(o, s);
                    };
                }
            }, this.tree = new c("div", {}), this.tree.root().set_class("row").child("label", {
                text: e
            }).set_class("description").up().child("div", {}).set_class("input").child("select", {}).bind(r).directive("bind", r).up().up();
        };
    }, {
        "nano-dom.js": 138,
        "virtual-dom.js": 26
    } ],
    175: [ function(a, t, e) {
        "use strict";
        var l = a("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = a("form-widgets.js").single_apmib_checkbox_form, i = {}, s = e.attr, e = s.text || "", o = s.mib || "", r = this.exports = {};
            this.obj = {
                created: function() {
                    r.checkbox = i.el, r.form = n(i.el, o);
                }
            }, this.tree = new l("div", {}), this.tree.root().set_class("row single-checkbox").text(" \n\t\t").child("checkbox", {}).bind(i).directive("bind", i).up().child("label", {
                text: e
            }).up();
        };
    }, {
        "form-widgets.js": 9,
        "virtual-dom.js": 26
    } ],
    176: [ function(n, t, e) {
        "use strict";
        var i = n("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var o = n("nbn_lib.js").getHttpRequest, s = {};
            this.obj = {
                created: function() {
                    var t = s.exports;
                    window.page_notify = t;
                    for (var e = {
                        run: function() {
                            this.pending = t.status_pending("??N??????µ???µ?????µ ???°N?N?N????µ??");
                        },
                        error: function() {
                            this.pending.stop(), t.error("??N????±???° ??N??????µ???µ????N? N???N???N?");
                        },
                        good: function() {
                            this.pending.stop(), t.good("????N???????"), setTimeout(function() {
                                window.location.href = window.location.href;
                            }, 2e3);
                        },
                        stop: function() {
                            this.pending.stop();
                        }
                    }, n = document.getElementsByTagName("form"), i = 0; i < n.length; i++) n[i].addEventListener("submit", function(t) {
                        console.log(this), console.log(this.action), e.run(), t.preventDefault(), console.log(t), 
                        console.log(this), function(i) {
                            for (var s = "", t = 0; t < i.elements.length; t++) {
                                var e = i.elements[t];
                                "submit" == e.type && 0, "checkbox" != e.type && "radio" != e.type || !e.checked ? "checkbox" != e.type && "radio" != e.type && (s += encodeURIComponent(i.elements[t].name) + "=" + encodeURIComponent(i.elements[t].value), 
                                t != i.elements.length - 1 && (s += "&")) : (s += encodeURIComponent(i.elements[t].name) + "=" + encodeURIComponent(i.elements[t].value), 
                                t != i.elements.length && (s += "&"));
                            }
                            return "multipart/form-data" == i.enctype && (s = new FormData(i)), new Promise(function(t, e) {
                                var n = o();
                                n.open("POST", i.action, !0), "application/x-www-form-urlencoded" == i.enctype && n.setRequestHeader("Content-type", i.enctype), 
                                n.addEventListener("load", function() {
                                    n.status < 400 ? (t(n.responseText), console.dir(n.responseText)) : (e(new Error("Request failed: " + n.statusText)), 
                                    console.dir(n.statusText));
                                }), n.addEventListener("error", function() {
                                    e(new Error("Network error"));
                                }), n.send(s);
                            });
                        }(this).then(function(t) {
                            e.good();
                        }).catch(function(t) {
                            e.error(), console.dir(t);
                        });
                    });
                }
            }, this.tree = new i("ros-error-log", {}), this.tree.root().bind(s).directive("bind", s);
        };
    }, {
        "nbn_lib.js": 18,
        "virtual-dom.js": 26
    } ],
    177: [ function(j, t, e) {
        "use strict";
        var k = j("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = j("event-emitter.js").EventEmiter, i = j("spa-lang.js").lang, s = j("system.js"), o = i();
            function r() {
                return s.rpc("rpc_apmib_get", {
                    list: [ "udpxyEnable", "udpxyPort", "udpxyBufferSize", "udpxyTimeout" ]
                });
            }
            var a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = {}, _ = new n(), f = this.exports = {}, m = [ c, u, d, h ], v = [ c ], b = v;
            function g() {
                0 == b.filter(function(t) {
                    return !t.exports.is_valid();
                }).length ? _.emit("form-valid", {}) : _.emit("form-invalid", {});
            }
            function x(t) {
                c.exports.set_value(t.udpxyEnable), u.exports.set_value(t.udpxyBufferSize), d.exports.set_value(t.udpxyPort), 
                h.exports.set_value(t.udpxyTimeout), m.forEach(function(t) {
                    return t.exports.no_changed();
                }), b = t.updxyEnable ? m : v, l.el.show(t.udpxyEnable);
            }
            function w() {
                p.el.disabled(!1), a.exports.good(o.notify.done), setTimeout(function() {
                    a.exports.clear();
                }, 2e3);
            }
            function y() {
                var t = {};
                t.udpxyEnable = c.exports.get_value(), t.udpxyEnable && (u.exports.is_changed() && (t.udpxyBufferSize = parseInt(u.exports.get_value(), 10)), 
                d.exports.is_changed() && (t.udpxyPort = parseInt(d.exports.get_value(), 10)), h.exports.is_changed() && (t.udpxyTimeout = parseInt(h.exports.get_value(), 10))), 
                m.forEach(function(t) {
                    return t.exports.no_changed();
                }), p.el.disabled(!1), a.exports.status_pending(o.notify.send), _.emit("save", t);
            }
            _.on("form-valid", function(t) {
                return p.el.disabled(!1);
            }), _.on("form-invalid", function(t) {
                return p.el.disabled(!0);
            }), this.obj = {
                created: function() {
                    r().then(x), c.exports.on("change", function() {
                        return l.el.show(c.exports.get_value());
                    }), m.forEach(function(t) {
                        return t.exports.on("change", g);
                    }), p.el.disabled(!0), f.on = function(t, e) {
                        return _.on(t, e);
                    }, f.set_data = x, f.save_end = w, p.el.on("click", y), _.emit("created", {}), _.on("save", function(t) {
                        s.rpc("rpc_apmib_set", {
                            list: t
                        }).then(function() {
                            return s.rpc("apply", {});
                        }).then(r).then(x).then(w);
                    });
                }
            }, this.tree = new k("blockquote", {}), this.tree.root().child("h2", {
                text: o.udpxy.title
            }).up().child("abstract", {
                text: o.udpxy.description
            }).up().text("\n \t\t").child("ros-error-log", {}).bind(a).directive("bind", a).up().child("ros-checkbox-row", {
                text: o.udpxy.enable
            }).bind(c).directive("bind", c).up().child("div", {}).bind(l).directive("bind", l).child("input-text-row", {
                text: o.udpxy.buffer
            }).bind(u).directive("bind", u).up().child("input-text-row", {
                text: o.udpxy.proxy_port
            }).bind(d).directive("bind", d).up().child("input-text-row", {
                text: o.udpxy.timeout
            }).bind(h).directive("bind", h).up().up().child("submit", {
                text: o.buttons.save
            }).bind(p).directive("bind", p).up();
        };
    }, {
        "event-emitter.js": 8,
        "spa-lang.js": 160,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    178: [ function(v, t, e) {
        "use strict";
        var b = v("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            var n = v("system.js"), i = n.rpc, s = n.poll, o = v("nano-dom.js"), r = (0, v("data_utility.js").get_capabilities)(), a = {}, l = {}, c = {}, u = {}, d = {}, p = {}, h = document.head.getElementsByTagName("link");
            function _(t, e) {
                var n = o.table().setClass("rt_table auto"), i = n.newRow().setClass("tbl_head");
                return t.forEach(function(t) {
                    return i.newCell().add(t);
                }), e.map(function(t) {
                    var e = n.newRow();
                    t.forEach(function(t) {
                        return e.newCell().add(t);
                    });
                }), n;
            }
            function f(n) {
                n[0].forEach(function(e) {
                    var t = n[1].filter(function(t) {
                        return t.ip == e.ip;
                    });
                    0 < t.length ? e.rssi = 0 == t[0].rssi ? "???°?±?µ?»N?" : t[0].rssi : e.rssi = "???¶?????°?????µ...";
                });
                return {
                    header: [ "IP-?°??N??µN?", "MAC-?°??N??µN?", "??N??????µ??N? N????????°?»?°" ],
                    data: n[0].map(function(t) {
                        return [ o.a("http://" + t.ip).set(t.ip).attr("target", "_blank"), t.mac, t.rssi ];
                    })
                };
            }
            function m() {
                return Promise.all([ i("unitySlaves_get", {}), i("unityGetSlavesInfo", {}) ]).then(function(t) {
                    console.log(t);
                    var e = f(t), t = e.header, e = e.data;
                    p.el.set(_(t, e));
                });
            }
            this.obj = {
                created: function() {
                    h[0].href = "style.css", u.el.show(!1), l.el.show(!1), c.el.show(!1), c.el.on("click", function() {
                        a.exports.status_pending("?˜???µN? ?????????»N?N??µ?????µ ?? ???±N??µ???????µ???????? N??µN???. ?????????¶????N??µ"), c.el.disabled(!0), 
                        i("unityStartAutoConfig", {});
                    }), console.log("start_wds_config", c);
                },
                mounted: function() {
                    var t;
                    console.log("data", r), "MASTER" == r.unityStatus ? (d.exports.set_value("????N????µ???°N? N???N????° ????N?N?N????°"), 
                    Promise.all([ i("unitySlaves_get", {}), [] ]).then(function(t) {
                        var e = f(t), t = e.header, e = e.data;
                        p.el.set(_(t, e));
                    }), s(1e3, m)) : "SLAVE" == r.unityStatus ? (d.exports.set_value("????N??µN???N?N? N???N????° ????N?N?N????°"), 
                    t = r.ipMaster, u.exports.set_value(o.a("http://" + t).set(t).attr("target", "_blank")), 
                    u.el.show(!0)) : "OFF" == r.unityStatus ? (d.exports.set_value("????N????° ???µ ?????????»N?N??µ???° ?? ???±N??µ???????µ???????? N??µN???"), 
                    l.el.show(!0), c.el.show(!0)) : (d.exports.set_value("????N????° ???µ ?????????»N?N??µ???° ?? ???±N??µ???????µ???????? N??µN???"), 
                    l.exports.set_value(""));
                }
            }, this.tree = new b("blockquote", {}), this.tree.root().child("h2", {}).text("???±N??µ???????µ?????°N? N??µN?N?").up().child("abstract", {}).up().text("\n \t\t").child("ros-error-log", {}).bind(a).directive("bind", a).up().child("row-info", {}).bind(d).directive("bind", d).text("??N??°N?N?N? N???N????? ????N?N?N????°:").up().child("row-info", {}).bind(u).directive("bind", u).text("IP ?°??N??µN? ????N????µ?????? N???N????? ????N?N?N????°:").up().child("p", {}).bind(l).directive("bind", l).text("??N? ?????¶?µN??µ ?????????»N?N???N?N?N?N? ?? ???±N??µ???????µ???????? N??µN??? ??N??????»N??·N?N? ??????????N? wps. ???»N? N?N??????? ??N??¶???? ???°?¶?°N?N? ?? N????µN??¶?????°N?N? ??????????N? wps ???° ????N????µ?????? ?? ????N??µN????µ?? N???N????µ, ????N??»?µ N??µ???? ??N??????·???????µN? ???±N??µ???????µ?????µ N???N??µ?? ????N?N?N????° ?? ???±N??µ???????µ????N?N? N??µN?N?. ?˜?»?? ??N? ?????¶?µN??µ ??N??????»N??·?????°N?N? ??????????N? ").child("span", {}).set_class("text-bold").text('"?????????»N?N???N?N?N?N? ?? ???±N??µ???????µ???????? N??µN???". ').up().up().child("button", {}).set_class("mt-20px").bind(c).directive("bind", c).text("?????????»N?N???N?N?N?N? ?? ???±N??µ???????µ???????? N??µN???").up().child("div", {}).set_class("mt-20px").bind(p).directive("bind", p).up();
        };
    }, {
        "data_utility.js": 4,
        "nano-dom.js": 138,
        "system.js": 23,
        "virtual-dom.js": 26
    } ],
    179: [ function(g, t, e) {
        "use strict";
        var x = g("virtual-dom.js").VirtualDom;
        t.exports.Ctor = function(t, e) {
            g("nano-json-rpc-2.js");
            var n = g("dom-maker.js"), i = (n.ex_ip_2_table, n.opts_act, g("error-handler.js")), s = i.Syslog, o = i.LOG, r = (g("validations.js").chain_valid, 
            g("validations.js").state_to_str), a = g("nano-dom.js"), l = new s("ExIpFilter", {
                level: o.DEBUG
            }), c = {}, u = {}, d = {}, p = {}, h = {}, _ = {}, f = [ _ ], m = [ _ ], n = {}, v = document.head.getElementsByTagName("link");
            i = {
                extract: function(t, e) {
                    var n = m.filter(function(t) {
                        return "" != t.exports.input.el.e.value;
                    });
                    return e.data(n, {
                        code: "post_exctract_no_empty"
                    }).valid("no_empty", {
                        required: !0
                    });
                }
            }, s = {
                run: function() {
                    this.pending = c.exports.status_pending("??N???N??°?????° ???°????N?N?");
                },
                stop: function() {
                    this.pending.stop();
                }
            };
            function b() {
                c.exports.good("????N???????"), setTimeout(function() {
                    c.exports.clear();
                }, 2e3);
            }
            this.obj = {
                mounted: function() {
                    v[0].href = "style.css", u.exports.checkbox.on("click", function() {
                        d.el.show(u.exports.checkbox.e.checked);
                    });
                }
            }, this.tree = new x("blockquote", {}), this.tree.root().child("h2", {}).text("URL ?¤???»N?N?N?").up().child("abstract", {}).text("?¤???»N?N?N? URL ??N??????»N??·N??µN?N?N?, N?N????±N? ?·?°??N??µN???N?N? ?????»N??·?????°N??µ?»N??? LAN ????N?N?N??? ?? ?˜??N??µN????µN?N?. ???»??????N?N?N?N?N?N? N??µ URL-?°??N??µN??°, ????N???N?N??µ N??????µN??¶?°N? ???»N?N??µ??N??µ N??»?????°, ???µN??µN???N??»?µ????N??µ ?????¶?µ.").up().text("\n \t\t").child("ros-error-log", {}).bind(c).directive("bind", c).up().child("proxy-form", {
                error_handler: function(t) {
                    l.log(o.INFO, t), t && t.code && alert("??N????±???° ??N??????µ???µ????N? N???N???N?");
                },
                submit: h,
                pending: s,
                after_good_submit: b,
                sub_forms: function() {
                    return [ u, d ];
                },
                after_update: function() {
                    d.el.show(u.exports.checkbox.e.checked), p.exports.update();
                }
            }).bind(n).directive("bind", n).child("single-apmib-checkbox", {
                mib: "urlFilterEnabled"
            }).bind(u).directive("bind", u).text("?????»N?N???N?N? URL ?¤???»N?N?N?").up().child("exctractor", {
                validator_res_handler: function(t) {
                    if (t.good()) return c.exports.clear(), !0;
                    var e = t.get_state();
                    return console.log([ "errors_log", e ]), e.msg.code && "post_exctract_no_empty" == e.msg.code ? c.exports.error("???? ???????? ?·???°N??µ?????µ ???µ ?·?°???°????.") : (t = e.msg.endsWith(":") ? e.msg.slice(0, -1) : e.msg, 
                    c.exports.error(t + " " + r(e.state))), !1;
                },
                inputs: f,
                post_exctract: i,
                method: "UrlFilterlist__add"
            }).bind(d).directive("bind", d).child("input-text-row", {
                name: "url"
            }).bind(_).directive("bind", _).text("URL-?°??N??µN?").up().up().child("submit", {}).bind(h).directive("bind", h).text("????N?N??°????N?N? ?? ??N??????µ????N?N?").up().up().child("ros-rm-list", {
                pending: s,
                after_good_submit: b,
                get_list: function(t) {
                    return {
                        header: [ "URL ?°??N??µN?" ],
                        data: t.map(function(t) {
                            return [ a.a("http://" + t.url).set(t.url) ];
                        })
                    };
                },
                table_name: "UrlFilterlist"
            }).bind(p).directive("bind", p).up();
        };
    }, {
        "dom-maker.js": 5,
        "error-handler.js": 7,
        "nano-dom.js": 138,
        "nano-json-rpc-2.js": 139,
        "validations.js": 25,
        "virtual-dom.js": 26
    } ],
    180: [ function(t, e, n) {
        e.exports.cacheing = function(t, n, i) {
            return function() {
                return t().then(function(t) {
                    var e = JSON.stringify(t);
                    i != e && (i = e, n(t));
                });
            };
        }, e.exports.AutoCache = function(n) {
            var i = "";
            this.caching = function(t) {
                var e = JSON.stringify(t);
                i != e && (i = e, n(t));
            };
        };
    }, {} ]
}, {}, [ 152 ]);
