# -*- coding: utf-8 -*-
"""Generate the four dashboard mockups (1280x720) for design approval."""
import math, os

OUT = os.path.dirname(os.path.abspath(__file__))
NAVY="#0B2545"; BLUE="#13315C"; TEAL="#1B7F79"; GOLD="#C9A227"; SLATE="#8FA3BF"
GRAY="#5C6B73"; RED="#B23A48"; SAND="#F4F6F8"; LINE="#E2E7EE"; WHITE="#FFFFFF"

def esc(s): return s

def header(title, active):
    tabs=[("Resumen",1),("Clientes y riesgo",2),("Tiempo real",3),("Gobierno",4)]
    nav="".join(f'<div class="tab {"on" if n==active else ""}">{t}</div>' for t,n in tabs)
    return f'''<div class="head">
      <div class="mark">PA</div>
      <div class="brand"><b>PAYLOAD ANALYTICS</b><span>Fintech LATAM · 7 países</span></div>
      <div class="ptitle">{title}</div>
      <div class="nav">{nav}</div>
      <div class="date">Datos al 29 oct 2026</div>
    </div>'''

def filters(status="Actualizado hace 2 h", dot=GOLD):
    return f'''<div class="filters">
      <div class="pill"><span>País</span><b>Todos</b><i>▾</i></div>
      <div class="pill"><span>Segmento</span><b>Todos</b><i>▾</i></div>
      <div class="pill"><span>Periodo</span><b>Últimos 12 meses</b><i>▾</i></div>
      <div class="fresh"><span class="dot" style="background:{dot}"></span>{status}</div>
    </div>'''

def footer(n):
    return f'''<div class="foot"><span>Fuente: Gold · PostgreSQL · 206 reglas dbt · cifras ilustrativas</span><span>{n:02d} / 04</span></div>'''

def kpi(x,y,w,label,value,delta,accent,spark=None,sub=""):
    sp=""
    if spark:
        pts=" ".join(f"{i*10},{28-v}" for i,v in enumerate(spark))
        sp=f'<svg class="spark" viewBox="0 0 90 30"><polyline points="{pts}" fill="none" stroke="{accent}" stroke-width="1.6"/></svg>'
    return f'''<div class="kpi" style="left:{x}px;top:{y}px;width:{w}px">
      <div class="accent" style="background:{accent}"></div>
      <div class="kl">{label}</div><div class="kv">{value}</div>
      <div class="kd">{delta}</div>{sp}{f'<div class="ks">{sub}</div>' if sub else ''}
    </div>'''

def card(x,y,w,h,title,kicker,body):
    return f'''<div class="card" style="left:{x}px;top:{y}px;width:{w}px;height:{h}px">
      <div class="ch"><div><div class="ct">{title}</div><div class="ck">{kicker}</div></div></div>
      <div class="cb">{body}</div>
    </div>'''

def hbars(items, maxv, color=NAVY, unit="", w=100, colors=None):
    out=[]
    for i,(label,val,txt) in enumerate(items):
        pct=max(2, val/maxv*100)
        c=colors[i] if colors else color
        out.append(f'<div class="hbar"><span class="hl">{label}</span><div class="ht"><div class="hf" style="width:{pct:.1f}%;background:{c}"></div></div><span class="hv">{txt}</span></div>')
    return "".join(out)

def donut(segments, center, sub, size=132):
    # segments: list of (label, pct, color)
    acc=0; stops=[]
    for _,p,c in segments:
        stops.append(f"{c} {acc:.2f}% {acc+p:.2f}%"); acc+=p
    bg="conic-gradient("+",".join(stops)+")"
    legend="".join(f'<div class="lg"><span class="sw" style="background:{c}"></span><span class="ln">{l}</span><b>{p:.0f}%</b></div>' for l,p,c in segments)
    return f'''<div class="donutwrap"><div class="donut" style="background:{bg};width:{size}px;height:{size}px"><div class="hole"><b>{center}</b><span>{sub}</span></div></div><div class="legend">{legend}</div></div>'''

def line_chart(values, labels, ymax, color=NAVY, w=560, h=150, target=None, values2=None, color2=TEAL, fill=True, fmt=lambda v:f"{v:.1f}"):
    left,right,top,bottom=34,8,8,20
    pw=w-left-right; ph=h-top-bottom
    n=len(values)
    def pts(vals):
        return [(left+i*pw/(n-1), top+ph*(1-v/ymax)) for i,v in enumerate(vals)]
    p=pts(values)
    path="M "+" L ".join(f"{x:.1f},{y:.1f}" for x,y in p)
    area=f"M {p[0][0]:.1f},{top+ph:.1f} L "+" L ".join(f"{x:.1f},{y:.1f}" for x,y in p)+f" L {p[-1][0]:.1f},{top+ph:.1f} Z"
    g=[]
    for i in range(5):
        yy=top+ph*i/4; val=ymax*(1-i/4)
        g.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{w-right}" y2="{yy:.1f}" stroke="{LINE}" stroke-width="1"/>')
        g.append(f'<text x="{left-6}" y="{yy+3:.1f}" text-anchor="end" class="ax">{fmt(val)}</text>')
    xl="".join(f'<text x="{left+i*pw/(n-1):.1f}" y="{h-5}" text-anchor="middle" class="ax">{lab}</text>' for i,lab in enumerate(labels) if i%2==0)
    extra=""
    if values2:
        p2=pts(values2)
        extra = '<path d="M ' + " L ".join(f"{x:.1f},{y:.1f}" for x,y in p2) + f'" fill="none" stroke="{color2}" stroke-width="2"/>'
        extra+="".join(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.4" fill="{color2}"/>' for x,y in p2[-1:])
    tgt=""
    if target:
        ty=top+ph*(1-target/ymax)
        tgt=f'<line x1="{left}" y1="{ty:.1f}" x2="{w-right}" y2="{ty:.1f}" stroke="{RED}" stroke-width="1" stroke-dasharray="4 3"/><text x="{left+4}" y="{ty-4:.1f}" text-anchor="start" class="ax red">objetivo {target:g}</text>'
    fill_el=f'<path d="{area}" fill="url(#grad)" opacity=".9"/>' if fill else ""
    return f'''<svg class="chart" viewBox="0 0 {w} {h}" preserveAspectRatio="none">
      <defs><linearGradient id="grad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="{color}" stop-opacity=".22"/><stop offset="100%" stop-color="{color}" stop-opacity="0"/></linearGradient></defs>
      {''.join(g)}{fill_el}<path d="{path}" fill="none" stroke="{color}" stroke-width="2.2"/>{extra}{tgt}{xl}
      <circle cx="{p[-1][0]:.1f}" cy="{p[-1][1]:.1f}" r="3.2" fill="{color}"/>
    </svg>'''

def columns(values, labels, colors=NAVY, h=120, highlight=None, counts=None):
    maxv=max(values); out=[]
    if isinstance(colors, str): colors=[colors]*len(values)
    for i,v in enumerate(values):
        c=colors[i] if i < len(colors) else NAVY
        cval=f'<div class="cval">{counts[i]:,}</div>'.replace(",", ".") if counts else ""
        out.append(f'<div class="col">{cval}<div class="cbar" style="height:{max(3,v/maxv*100):.1f}%;background:{c}"></div><span>{labels[i]}</span></div>')
    return f'<div class="cols" style="height:{h}px">'+ "".join(out)+'</div>'

def heat():
    rows=[("premium",[22,19,27,31,24,28,21]),("retail",[31,28,35,38,33,36,30]),("sme",[26,24,29,34,27,31,25]),("private",[14,12,18,21,16,19,13])]
    countries=["AR","BR","CL","CO","MX","PE","UY"]
    head="".join(f'<th>{c}</th>' for c in countries)
    body=""
    for name,vals in rows:
        cells="".join(f'<td style="background:rgba(178,58,72,{0.10+ (v-10)/40*0.55:.2f})">{v}%</td>' for v in vals)
        body+=f'<tr><th class="rowh">{name}</th>{cells}</tr>'
    return f'<table class="heat"><thead><tr><th></th>{head}</tr></thead><tbody>{body}</tbody></table>'''

def table_fx():
    rows=[("USD","1,0000","seed","12 h","—"),("COP","0,000248","stream","3 s","+0,4%"),("MXN","0,05810","stream","2 s","-0,1%"),("BRL","0,18420","stream","2 s","+0,3%"),("ARS","0,000619","stream","1 s","+1,2%"),("CLP","0,001048","seed","11 h","—"),("PEN","0,26500","seed","10 h","—"),("UYU","0,02500","seed","10 h","—"),("EUR","1,08200","stream","3 s","-0,2%")]
    body="".join(f'<tr><td class="cur">{c}</td><td class="num">{r}</td><td><span class="tag {s}">{s}</span></td><td class="num muted">{a}</td><td class="num {"pos" if d.startswith("+") else ("neg" if d.startswith("-") else "muted")}">{d}</td></tr>' for c,r,s,a,d in rows)
    return f'<table class="fx"><thead><tr><th>Moneda</th><th>Tasa USD</th><th>Fuente</th><th>Edad</th><th>Δ seed</th></tr></thead><tbody>{body}</tbody></table>'

# ---------- pages ----------
def page1():
    body = kpi(24,124,236,"Clientes","5.100","+2,4% vs mes anterior",NAVY,[10,11,12,12,13,14,14,15]) \
      + kpi(273,124,236,"Saldo total USD","3,24 B","+1,8% vs mes anterior",TEAL,[10,10,11,11,12,13,13,13]) \
      + kpi(522,124,236,"Ingresos USD","78,4 M","+6,2% vs mes anterior",GOLD,[9,10,10,11,11,12,13,14]) \
      + kpi(771,124,236,"Mora &gt; 90 días","25,1%","-0,8 pp vs mes anterior",RED,[18,17,17,16,16,15,15,14],sub="1.920 de 7.663 créditos") \
      + kpi(1020,124,236,"Transacciones","87.686","+4,1% vs mes anterior",SLATE,[10,11,11,12,12,13,14,14])
    body += card(24,232,620,220,"Ingresos por mes","USD millones · meta 7,0 M · últimos 12 meses", line_chart([4.8,5.2,5.6,5.1,5.9,6.2,6.0,6.4,6.8,6.5,7.1,7.4],["nov","dic","ene","feb","mar","abr","may","jun","jul","ago","sep","oct"],8,NAVY,560,150,target=7.0,fmt=lambda v:f"{v:.0f}"))
    body += card(660,232,596,220,"Mezcla por segmento","Participación sobre ingresos", donut([("Premium",32,NAVY),("Retail",28,TEAL),("SME",24,GOLD),("Private banking",16,SLATE)],"78,4 M","Ingresos",132))
    body += card(24,468,620,204,"Saldo por país","USD · millones", hbars([("MX",660,"0,66 B"),("PE",660,"0,66 B"),("BR",630,"0,63 B"),("UY",370,"0,37 B"),("CO",310,"0,31 B"),("AR",310,"0,31 B"),("CL",290,"0,29 B")],700,NAVY))
    body += card(660,468,290,204,"Transacciones por tipo","Participación del volumen", hbars([("Transfer",32,"32%"),("Compra",24,"24%"),("Retiro",18,"18%"),("Fee",9,"9%"),("Pago",4,"4%")],40,TEAL))
    body += card(966,468,290,204,"Crecimiento","Variación mensual", '<div class="big">+6,2%</div><div class="subtle">Ingresos vs septiembre</div><svg class="mini" viewBox="0 0 240 54"><polyline points="0,44 30,40 60,42 90,34 120,30 150,33 180,22 210,18 240,12" fill="none" stroke="'+GOLD+'" stroke-width="2.4"/></svg><div class="smallrow"><span>Ticket promedio</span><b>894 USD</b></div><div class="smallrow"><span>Ingreso / cliente</span><b>15.373 USD</b></div>')
    return body

def page2():
    body = kpi(24,124,299,"Score promedio","569","3.745 clientes con score",TEAL,[10,11,11,12,12,13,13,14]) \
      + kpi(335,124,299,"Mora &gt; 90 días","25,1%","1.920 de 7.663 créditos",RED,[18,18,17,17,16,16,15,15]) \
      + kpi(646,124,299,"Cartera USD","1,86 B","+0,9% vs mes anterior",NAVY,[10,10,11,11,12,12,13,13]) \
      + kpi(957,124,299,"DPD promedio","67 días","Máximo 365 días",GOLD,[15,15,16,16,17,17,18,18])
    body += card(24,232,620,220,"Distribución por banda de score","3.745 clientes con score válido", columns([18,24,31,19,8],["Poor","Fair","Good","Very good","Exceptional"],[RED,"#D08C3C",SLATE,TEAL,NAVY],150,counts=[674,899,1161,712,300]) + '<div class="note">1.355 clientes sin score válido (fuera del rango 350–800)</div>')
    body += card(660,232,596,220,"Mora 90+ por segmento y país","% de créditos sobre el total del segmento", heat())
    body += card(24,468,400,204,"Mora por país","% de créditos con DPD ≥ 90", hbars([("AR",30,"30,2%"),("BR",28,"28,4%"),("MX",27,"26,8%"),("CO",25,"25,1%"),("PE",24,"24,3%"),("CL",23,"23,0%"),("UY",22,"22,1%")],32,RED))
    body += card(440,468,400,204,"Cartera por producto","USD · participación", donut([("Personal",38,NAVY),("Hipotecario",27,TEAL),("Auto",21,GOLD),("Empresarial",14,SLATE)],"1,86 B","Cartera",104))
    body += card(856,468,400,204,"Buckets DPD","Créditos por días de mora", hbars([("0",2400,"2.400"),("1–30",1100,"1.100"),("31–60",900,"900"),("61–90",1300,"1.300"),("90+",1920,"1.920")],2500,RED,colors=[SLATE,GOLD,"#D08C3C",RED,"#7A1F2B"]))
    return body

def page3():
    body = kpi(24,124,299,"Latencia p95","2,8 s","objetivo < 5 s",TEAL,[16,14,13,12,12,11,10,10]) \
      + kpi(335,124,299,"Transacciones / hora","7.240","última ventana de 60 min",NAVY,[10,11,11,12,12,13,13,14]) \
      + kpi(646,124,299,"Volumen 24 h","1,24 M USD","12.480 transacciones",GOLD,[9,10,10,11,11,12,12,13]) \
      + kpi(957,124,299,"Monedas","9","4 con tasa de mercado",SLATE)
    body += card(24,232,760,220,"Latencia del streaming","p50 1,3 s · p95 2,8 s · objetivo 5 s", line_chart([1.1,1.3,1.2,1.4,1.3,1.2,1.5,1.4,1.3,1.2,1.4,1.3],["16:20","","16:25","","16:30","","16:35","","16:40","","16:45",""],6,TEAL,700,155,target=5,values2=[2.4,2.9,2.6,3.1,2.8,2.6,3.2,2.9,2.7,2.8,3.0,2.8],color2=NAVY))
    body += card(800,232,456,220,"Fuente de la tasa","Transacciones con tasa en ventana", donut([("Event window",86,TEAL),("Fallback",14,GOLD)],"86%","en ventana",110) + '<div class="statusline"><span class="dot" style="background:'+TEAL+'"></span>Streaming conectado · último evento hace 18 s</div>')
    body += card(24,468,760,204,"Volumen por minuto","Transacciones · últimos 15 minutos", columns([92,104,96,118,126,110,132,142,128,150,138,146,155,148,160],["16:31","","16:33","","16:35","","16:37","","16:39","","16:41","","16:43","","16:45"],NAVY,132,GOLD))
    body += card(800,468,456,204,"Tasas vigentes por moneda","USD por unidad · última ventana", table_fx())
    return body

def page4():
    body = kpi(24,124,236,"Reglas de calidad","206","9 tipos · dbt test",NAVY,[10,10,10,10,10,10,10,10]) \
      + kpi(273,124,236,"Cumplimiento","100%","0 fallos en la última corrida",TEAL,[10,10,10,10,10,10,10,10]) \
      + kpi(522,124,236,"Registros marcados","10.738","se conservan y se señalan",GOLD,[10,11,11,12,12,13,13,14]) \
      + kpi(771,124,236,"Emails inválidos","1.842","36,1% de los clientes",RED,[12,13,13,14,14,15,15,16]) \
      + kpi(1020,124,236,"Frescura streaming","18 s","último processed_at",TEAL,[10,10,10,10,10,10,10,10])
    body += card(24,232,620,220,"Reglas por tipo","206 reglas activas en cada corrida", hbars([("No nulos",90,"90"),("No negativos",30,"30"),("Aceptados",19,"19"),("Relaciones",17,"17"),("Únicos",14,"14"),("Rangos",11,"11"),("Fechas",6,"6"),("Positivos",5,"5"),("Negocio",14,"14")],100,NAVY))
    body += card(660,232,596,220,"Monedas inconsistentes por país","8.896 de 17.529 cuentas (50,8%) · moneda distinta a la esperada", hbars([("BR",1420,"1.420"),("PE",1380,"1.380"),("MX",1330,"1.330"),("AR",1290,"1.290"),("CO",1240,"1.240"),("UY",1180,"1.180"),("CL",1056,"1.056")],1500,RED))
    body += card(24,468,760,204,"Frescura por dominio","Última marca disponible y antigüedad", '''<table class="freshtable"><thead><tr><th>Dominio</th><th>Última marca</th><th>Antigüedad</th><th>Estado</th></tr></thead><tbody>
      <tr><td>Gold batch</td><td>29 oct 2026 · 10:12</td><td>2 h 14 min</td><td><span class="tag warn">programado</span></td></tr>
      <tr><td>Streaming transactions</td><td>29 oct 2026 · 16:45</td><td>18 s</td><td><span class="tag ok">al día</span></td></tr>
      <tr><td>FX rates</td><td>29 oct 2026 · 16:45</td><td>4 s</td><td><span class="tag ok">al día</span></td></tr>
      <tr><td>dbt tests</td><td>29 oct 2026 · 10:20</td><td>206 / 206</td><td><span class="tag ok">ok</span></td></tr>
    </tbody></table>''')
    body += card(800,468,456,204,"Gobierno del dato","Cobertura y clasificación", '''<div class="gov"><div class="ring"><svg viewBox="0 0 80 80"><circle cx="40" cy="40" r="30" fill="none" stroke="#E2E7EE" stroke-width="8"/><circle cx="40" cy="40" r="30" fill="none" stroke="#1B7F79" stroke-width="8" stroke-dasharray="188.5" stroke-dashoffset="0" transform="rotate(-90 40 40)"/></svg><b>100%</b></div><div class="govlist"><div><b>375</b> columnas documentadas</div><div><b>17</b> PII directa · <b>43</b> financieras</div><div><b>206</b> reglas en cada corrida</div><div class="subtle">Catálogo y linaje en docs/governance</div></div></div>''')
    return body

CSS = f'''
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{width:1280px;height:720px;overflow:hidden;background:{SAND};font-family:"Segoe UI",Arial,sans-serif;color:#1B1B1B;font-variant-numeric:tabular-nums}}
.page{{position:relative;width:1280px;height:720px;background:{SAND}}}
.head{{position:absolute;left:0;top:0;width:1280px;height:64px;background:{NAVY}}}
.mark{{position:absolute;left:24px;top:17px;width:30px;height:30px;border-radius:6px;background:{GOLD};color:{NAVY};font-weight:800;font-size:11px;display:flex;align-items:center;justify-content:center;letter-spacing:.5px}}
.brand{{position:absolute;left:64px;top:14px;color:#fff}}
.brand b{{display:block;font-size:10.5px;letter-spacing:1.7px;font-weight:700}}
.brand span{{display:block;font-size:8.5px;color:#AFC0D6;margin-top:2px;letter-spacing:.3px}}
.ptitle{{position:absolute;left:300px;top:21px;color:#fff;font-size:17px;font-weight:600;letter-spacing:.2px}}
.nav{{position:absolute;left:700px;top:0;height:64px;display:flex;align-items:center;gap:22px}}
.tab{{font-size:10.5px;color:#AFC0D6;height:64px;display:flex;align-items:center;border-bottom:2px solid transparent;white-space:nowrap}}
.tab.on{{color:#fff;font-weight:600;border-bottom-color:{GOLD}}}
.date{{position:absolute;right:24px;top:19px;font-size:9.5px;color:#D6E0EE;border:1px solid rgba(255,255,255,.22);border-radius:14px;padding:6px 10px}}
.filters{{position:absolute;left:0;top:64px;width:1280px;height:56px;background:#fff;border-bottom:1px solid {LINE}}}
.pill{{position:absolute;top:13px;height:30px;border:1px solid #D9E0E9;border-radius:15px;padding:0 12px;display:flex;align-items:center;gap:6px;font-size:10px;background:#fff}}
.pill span{{color:{GRAY}}} .pill b{{color:{NAVY};font-weight:600}} .pill i{{font-style:normal;color:{GRAY};font-size:9px;margin-left:2px}}
.pill:nth-child(1){{left:24px}} .pill:nth-child(2){{left:170px}} .pill:nth-child(3){{left:326px}}
.fresh{{position:absolute;right:24px;top:20px;font-size:9.5px;color:{GRAY};display:flex;align-items:center;gap:6px}}
.dot{{width:7px;height:7px;border-radius:50%;display:inline-block}}
.foot{{position:absolute;left:24px;right:24px;bottom:10px;height:22px;border-top:1px solid {LINE};display:flex;align-items:flex-end;justify-content:space-between;font-size:9px;color:#7A8798}}
.kpi{{position:absolute;top:124px;height:92px;background:#fff;border:1px solid {LINE};border-radius:6px;box-shadow:0 1px 2px rgba(11,37,69,.05);padding:12px 14px 10px 16px;overflow:hidden}}
.kpi .accent{{position:absolute;left:0;top:0;width:3px;height:92px}}
.kl{{font-size:9px;text-transform:uppercase;letter-spacing:.7px;color:{GRAY};font-weight:600}}
.kv{{font-size:25px;line-height:31px;color:{NAVY};font-weight:650;letter-spacing:-.3px;margin-top:2px}}
.kd{{font-size:9px;color:{TEAL};margin-top:1px}}
.ks{{font-size:8px;color:#8A97A8;margin-top:1px}}
.spark{{position:absolute;right:10px;bottom:8px;width:80px;height:28px;opacity:.9}}
.card{{position:absolute;background:#fff;border:1px solid {LINE};border-radius:6px;box-shadow:0 1px 2px rgba(11,37,69,.05);overflow:hidden}}
.ch{{padding:12px 14px 0 14px;display:flex;justify-content:space-between}}
.ct{{font-size:12px;font-weight:650;color:{NAVY};letter-spacing:.1px}}
.ck{{font-size:8.5px;color:#8492A4;margin-top:2px;letter-spacing:.2px}}
.cb{{position:absolute;left:14px;right:14px;top:44px;bottom:10px}}
.chart{{width:100%;height:100%}}
.ax{{font-size:8px;fill:#8492A4;font-family:"Segoe UI",Arial,sans-serif}}
.ax.red{{fill:{RED}}}
.legendline{{position:absolute;left:0;bottom:-2px;display:flex;gap:14px;font-size:8.5px;color:{GRAY};align-items:center;width:100%}}
.legendline i{{width:9px;height:3px;display:inline-block;border-radius:2px;margin-right:4px;vertical-align:middle}}
.legendline i.dash{{background:repeating-linear-gradient(90deg,{RED} 0 4px,transparent 4px 7px);height:2px}}
.legendline .right{{margin-left:auto;color:{NAVY};font-weight:600}}
.hbar{{display:flex;align-items:center;gap:8px;height:18px;font-size:8.8px}}
.hl{{width:78px;color:{NAVY};font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.ht{{flex:1;height:8px;background:#F0F3F7;border-radius:3px;overflow:hidden}}
.hf{{height:8px;border-radius:3px}}
.hv{{width:46px;text-align:right;color:{GRAY};font-size:8.5px}}
.donutwrap{{display:flex;align-items:center;gap:18px;height:100%;justify-content:center}}
.donut{{border-radius:50%;position:relative;flex:none}}
.hole{{position:absolute;inset:16px;background:#fff;border-radius:50%;display:flex;flex-direction:column;align-items:center;justify-content:center}}
.hole b{{font-size:18px;color:{NAVY};font-weight:650}} .hole span{{font-size:8px;color:{GRAY};margin-top:2px}}
.legend{{display:flex;flex-direction:column;gap:7px;min-width:120px}}
.lg{{display:flex;align-items:center;gap:7px;font-size:9.5px;color:{GRAY}}}
.lg b{{margin-left:auto;color:{NAVY};font-weight:600}} .sw{{width:8px;height:8px;border-radius:2px;display:inline-block}}
.big{{font-size:34px;color:{TEAL};font-weight:650;letter-spacing:-1px;margin-top:2px}}
.subtle{{font-size:8.5px;color:#8492A4}}
.mini{{width:100%;height:54px;margin:6px 0 2px 0}}
.smallrow{{display:flex;justify-content:space-between;font-size:9px;color:{GRAY};padding:3px 0;border-top:1px solid #F0F3F7}}
.smallrow b{{color:{NAVY};font-weight:600}}
.cols{{display:flex;align-items:flex-end;gap:10px;padding:0 4px;height:150px}}
.col{{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%;gap:4px}}
.cbar{{width:70%;border-radius:3px 3px 0 0;min-height:3px}}
.cval{{font-size:7.5px;color:{NAVY};font-weight:600;margin-bottom:-1px}}
.col span{{font-size:8px;color:{GRAY};white-space:nowrap}}
.note{{position:absolute;left:0;bottom:-2px;font-size:8px;color:#8492A4}}
.heat{{width:100%;border-collapse:separate;border-spacing:3px;font-size:8.5px;text-align:center;margin-top:-4px}}
.heat th{{color:{GRAY};font-weight:600;font-size:8px;padding:2px}}
.heat td{{color:{NAVY};padding:7px 2px;border-radius:3px;font-weight:600}}
.heat .rowh{{text-align:left;color:{NAVY};font-size:8.5px;width:62px;background:transparent}}
.fx{{width:100%;border-collapse:collapse;font-size:8px}}
.fx th{{text-align:left;color:#8492A4;font-weight:600;font-size:7.2px;text-transform:uppercase;letter-spacing:.4px;padding:1px 3px;border-bottom:1px solid {LINE}}}
.fx td{{padding:1.5px 3px;line-height:12px;border-bottom:1px solid #F2F5F8;color:{NAVY}}}
.fx .cur{{font-weight:700}} .fx .num{{text-align:right}} .fx .muted{{color:#8492A4}} .fx .pos{{color:{TEAL};text-align:right}} .fx .neg{{color:{RED};text-align:right}}
.tag{{font-size:7.5px;padding:2px 5px;border-radius:8px;font-weight:600;text-transform:uppercase;letter-spacing:.2px}}
.tag.stream{{background:#E6F2F0;color:{TEAL}}} .tag.seed{{background:#F6F1DF;color:#8A6D12}} .tag.ok{{background:#E6F2F0;color:{TEAL}}} .tag.warn{{background:#F6F1DF;color:#8A6D12}}
.statusline{{position:absolute;left:0;bottom:-2px;font-size:8.5px;color:{GRAY};display:flex;align-items:center;gap:6px}}
.statusline .dot{{width:7px;height:7px}}
.freshtable{{border-collapse:collapse;width:100%;font-size:9px}}
.freshtable th{{text-align:left;color:#8492A4;font-size:8px;text-transform:uppercase;letter-spacing:.4px;padding:3px;border-bottom:1px solid {LINE}}}
.freshtable td{{padding:8px 3px;border-bottom:1px solid #F2F5F8;color:{NAVY}}}
.gov{{display:flex;align-items:center;gap:16px;height:100%}}
.ring{{position:relative;width:86px;height:86px;flex:none}}
.ring svg{{width:86px;height:86px}} .ring b{{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:15px;color:{TEAL}}}
.govlist{{display:flex;flex-direction:column;gap:7px;font-size:9px;color:{GRAY}}}
.govlist b{{color:{NAVY};font-size:11px}}
'''

def build():
    pages=[("01-resumen","Resumen ejecutivo",1,page1()),("02-clientes-riesgo","Clientes y riesgo crediticio",2,page2()),("03-tiempo-real","Operación en tiempo real",3,page3()),("04-gobierno","Calidad y gobierno del dato",4,page4())]
    for slug,title,n,body in pages:
        html=f'''<!doctype html><html lang="es"><head><meta charset="utf-8"><title>{title}</title><style>{CSS}</style></head><body><div class="page">{header(title,n)}{filters()}{body}{footer(n)}</div></body></html>'''
        open(os.path.join(OUT,slug+".html"),"w",encoding="utf-8").write(html)
        print("wrote",slug+".html")

if __name__=="__main__":
    build()

