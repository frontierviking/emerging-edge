import subprocess
import unittest
import dashboard


class ChartHoverTests(unittest.TestCase):
    def test_hover_tracks_rendered_vertices_at_responsive_widths(self):
        js = dashboard.JS
        render = js[js.index('function _fmtPriceCompact'):js.index('// Per-ticker cache')]
        hover = js[js.index('function _onChartMousemove'):js.index("document.addEventListener('mousemove', _onChartMousemove)")]
        setup = r'''
const assert = require('node:assert/strict');
const hist = [['2026-07-06', 270000], ['2026-08-20', 548000], ['2026-10-04', 400001]];
let indexed = false;
const document = {body: {classList: {contains: () => indexed}}};
const window = {innerWidth: 1200};
const _STOCK_CURRENCY = {'UTYK@UZSE':'UZS'};
let _CHART_HOVER_DATA = {}, _activeChartSvg = null;
const tip = {style:{}, offsetWidth:120};
function _ensureChartTip() {return tip;}
function _hideChartTip() {}
function _clearActiveDot() {}
'''
        checks = r'''
for (indexed of [false, true]) {
  const bounds = indexed ? {sharedMin:90,sharedMax:210} : {};
  const svgText = _renderChartSVG(hist, 'UZS', hist[0][0], hist[2][0], {indexed,...bounds});
  const vertices = /<polyline points="([^"]+)"/.exec(svgText)[1].split(' ').map(p=>p.split(',').map(Number));
  _CHART_HOVER_DATA['UTYK@UZSE'] = {hist,winStart:hist[0][0],winEnd:hist[2][0],...bounds};
  for (const width of [160,220,360]) {
    const scale = Math.min(width/220,1), offset = (width-220*scale)/2;
    const attrs = {};
    const dot = {setAttribute:(k,v)=>attrs[k]=Number(v)};
    const svg = {
      closest:()=>({dataset:{chartKey:'UTYK@UZSE'}}),
      getBoundingClientRect:()=>({left:30,width,height:120}),
      getScreenCTM:()=>({inverse:()=>({})}),
      createSVGPoint:()=>({x:0,y:0,matrixTransform(){return {x:(this.x-30-offset)/scale};}}),
      querySelector:()=>dot
    };
    for (let i=0;i<vertices.length;i++) {
      const [x,y] = vertices[i];
      _onChartMousemove({target:{closest:()=>svg},clientX:30+offset+x*scale,clientY:80});
      assert.equal(attrs.cx,x); assert.equal(attrs.cy,y);
      assert.ok(tip.textContent.includes(_fmtPriceCompact(hist[i][1])));
    }
  }
}
'''
        subprocess.run(['node', '-e', setup + render + hover + checks], check=True)

    def test_refresh_does_not_replace_chart_without_hover(self):
        js = dashboard.JS
        sync = js[js.index('async function _syncVisibleQuotes'):js.index('setInterval(_syncVisibleQuotes')]
        self.assertNotIn("'.chip-chart'", sync)
        self.assertIn('setGraphRange(', sync)
        seed = js[js.index('function _seedHoverCacheFromDefault'):js.index('_ensureStockHistory().then(_seedHoverCacheFromDefault)')]
        self.assertIn("localStorage.getItem('ee-graph-range')", seed)
        self.assertIn('setGraphRange(', seed)


if __name__ == '__main__':
    unittest.main()
