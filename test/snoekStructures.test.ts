import {
  buildScoutHotspots,
  SnoekStructure,
  SnoekStructureType
} from '../src/services/snoekStructures';

function structure(type: SnoekStructureType, cluster: number): SnoekStructure {
  const score = {
    pumping_station: 91,
    weir: 79,
    lock: 75,
    bridge: 63,
    fish_passage: 69,
    culvert: 59
  }[type] || 60;
  const x = cluster * 1.5;

  return {
    id: `${type}-${cluster}`,
    type,
    source: 'pdok-imwa',
    sourceLayer: type,
    name: `${type} ${cluster}`,
    label: type,
    lat: 52.36 + cluster * 0.001,
    lon: 4.55 + cluster * 0.001,
    geometry: {
      type: 'Point',
      coordinates: [4.55 + cluster * 0.001, 52.36 + cluster * 0.001]
    },
    x,
    y: 20,
    score,
    reasons: []
  };
}

describe('buildScoutHotspots', () => {
  it('keeps the selected hotspot categories balanced and independent', () => {
    const structures: SnoekStructure[] = [];
    for (let cluster = 0; cluster < 50; cluster += 1) {
      structures.push(
        structure('pumping_station', cluster),
        structure('weir', cluster),
        structure('lock', cluster),
        structure('bridge', cluster),
        structure('fish_passage', cluster),
        structure('culvert', cluster)
      );
    }

    const hotspots = buildScoutHotspots(structures, 40);
    const counts = hotspots.reduce((result, hotspot) => {
      result[hotspot.type] = (result[hotspot.type] || 0) + 1;
      return result;
    }, {} as Record<string, number>);

    expect(hotspots).toHaveLength(40);
    expect(counts).toEqual({
      pumping_station: 12,
      weir: 10,
      lock: 6,
      bridge: 6,
      fish_passage: 4,
      culvert: 2
    });
  });

  it('places each hotspot on a real source object instead of a cluster average', () => {
    const pump = structure('pumping_station', 2);
    const weir = structure('weir', 2);
    weir.lat += 0.004;
    weir.lon += 0.004;

    const hotspots = buildScoutHotspots([pump, weir], 10);
    const pumpHotspot = hotspots.find((hotspot) => hotspot.type === 'pumping_station');
    const weirHotspot = hotspots.find((hotspot) => hotspot.type === 'weir');

    expect([pumpHotspot?.lat, pumpHotspot?.lon]).toEqual([pump.lat, pump.lon]);
    expect([weirHotspot?.lat, weirHotspot?.lon]).toEqual([weir.lat, weir.lon]);
    expect(pumpHotspot?.geometry).toEqual(pump.geometry);
    expect(weirHotspot?.geometry).toEqual(weir.geometry);
  });
});
