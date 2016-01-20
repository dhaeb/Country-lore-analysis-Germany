var febjahr_geo_location = "https://raw.githubusercontent.com/dhaeb/Country-lore-analysis-Germany/master/febjahr_geo.json"

var pointVector, filterObj = [];
    
Array.prototype.max = function() {
return Math.max.apply(null, this);
};

Array.prototype.min = function() {
return Math.min.apply(null, this);
};

function HSVtoRGB(h, s, v) {
    var r, g, b, i, f, p, q, t;
    if (arguments.length === 1) {
	s = h.s, v = h.v, h = h.h;
    }
    i = Math.floor(h * 6);
    f = h * 6 - i;
    p = v * (1 - s);
    q = v * (1 - f * s);
    t = v * (1 - (1 - f) * s);
    switch (i % 6) {
	case 0: r = v, g = t, b = p; break;
	case 1: r = q, g = v, b = p; break;
	case 2: r = p, g = v, b = t; break;
	case 3: r = p, g = q, b = v; break;
	case 4: r = t, g = p, b = v; break;
	case 5: r = v, g = p, b = q; break;
    }
    return [Math.round(r * 255), 
    	    Math.round(g * 255), 
    	    Math.round(b * 255)];
 };


var normalStyle = new ol.style.Style({
	image: new ol.style.Circle({
                radius: 3,
                fill: new ol.style.Fill({
                    color: 'red'
                })
            })	
});

var invisble = new ol.style.Style({
	name : "invisible",
	image: new ol.style.Circle({
                radius: 0,
                fill: new ol.style.Fill({
                    color: [0,0,0,0]
                })
            })	
});
 
$(function() {
    var iconStyle = new ol.style.Style({
       image: new ol.style.Icon ({
           anchor: [0.5, 46],
           anchorXUnits: 'fraction',
           anchorYUnits: 'pixels',
           opacity: 0.65,
           // create commons license
           src: 'http://iconshow.me/download.phpl?file=path/media/images/Mixed/small-n-flat-icon/png2/48/-map-marker.png' 
       })
    });

    function createPointStyle(color, radius){
        return new ol.style.Style({
	    name : "default",
	    image: new ol.style.Circle({
                radius: radius,
                fill: new ol.style.Fill({
                    color: color
                })
            })
        });
    };

    var styleFunction = function (feature, resolution) {
        var rad = feature.get("cAll") / 10.0 + 0.5;
        var percentTrueCl = feature.get('rel');
        var color = HSVtoRGB(percentTrueCl,1,1); // choose color from red to red (but 0 is not a data value, so it wont be confused)
        color.push(1);
        return [createPointStyle(color, rad)];
    };

    var iconFeature = new ol.Feature({
        geometry: new ol.geom.Point(ol.proj.transform([13, 51], 'EPSG:4326','EPSG:3857')),
        name: "adsf",
        population: 4000,
        rainfall: 500
    });

    pointVector = new ol.source.Vector({
		      url : febjahr_geo_location,
		      format: new ol.format.GeoJSON()
    });

    var sliderLowerDate = "sliderLowerDate",
    sliderCount   = "sliderCount",
    sliderHeight = "sliderHeight",
    sliderHigherDate = "sliderHigherDate";

    pointVector.on("addfeature", function(vectorevent){
	    // erstelle Filterobjekt für alle Filter, dies es geben soll!
	    filterObj.push({
		    sliderHeight : false,
		    sliderCount : false,
		    sliderLowerDate : false,
		    sliderHeigherDate : false,
		    isFilteredOut : function() {return this.sliderHeight | this.sliderCount | this.sliderLowerDate | this.sliderHigherDate;}
	    });
	    vectorevent.feature.B.von = new Date(vectorevent.feature.B.von);
	    vectorevent.feature.B.bis = new Date(vectorevent.feature.B.bis); 		
    });      

    var attribution = new ol.control.Attribution({
      collapsible: true,
      label: 'A',
      collapsed: true,
      tipLabel: 'Filter and copyright info',
      target : 'attribution'
    });

    var map = new ol.Map({
    target: 'map',
    renderer: 'canvas',
    layers: [
      new ol.layer.Tile({
        source: new ol.source.MapQuest({layer : "osm"})
      }),
      new ol.layer.Vector({
      	name : "data",
      	source: pointVector,
      	style : styleFunction,	
      })
    ],                               
    view: new ol.View({
      center: new ol.proj.transform([10.41,51.30], "EPSG:4326", "EPSG:3857"),
      zoom: 7
    }),
     controls: ol.control.defaults({ attribution: false }).extend([attribution])
    });

    var element = document.getElementById('popup');

    /**
     * Elements that make up the popup.
     */
    var container = document.getElementById('popup');
    var content = document.getElementById('popup-content');
    var closer = document.getElementById('popup-closer');

    var popup = new ol.Overlay({
       element: container,
       autoPan : true,
       autoPanAnimation: {
         duration: 250
       },
       positioning: 'bottom-center',
       stopEvent: false
    });

    /**
     * Add a click handler to hide the popup.
     * @return {boolean} Don't follow the href.
     */
    closer.onclick = function() {
      popup.setPosition(undefined);
      closer.blur();
      return false;
    };

    map.addOverlay(popup);

    function createPopover(feature){
       return 	  "<table><tr><td>Ort: </td><td>" + feature.get('Ort') + "</td></tr>" 
          		+ "<tr><td>Bundesland: </td><td>" + feature.get('Bundesland') + "</td></tr>"
          		+ "<tr><td>Anzahl Messjahre: </td><td>" + feature.get('cAll') + "</td></tr>"
          		+ "<tr><td>Verhaeltnis: </td><td>" + feature.get('rel') + "</td></tr>"
          		+ "<tr><td>Lage: </td><td>" + feature.get('Lage') + "</td></tr>"
          		+ "<tr><td>Hoehe: </td><td>" + feature.get('Hoehe') + "</td></tr>"
          		+ "<tr><td>Von-Bis: </td><td>" + feature.get('von').getFullYear() + "-" + feature.get('bis').getFullYear() + "</td></tr>"
          		+ "</table>";
    };

    // display popup on click
    map.on('click', function(evt) {
      evt.preventDefault();
      var feature = map.forEachFeatureAtPixel(evt.pixel,
          function(feature, layer) {
	    return feature;
          });
      if (feature) {
	    var coordinate = evt.coordinate;
	    popup.setPosition(coordinate);
	    content.innerHTML = createPopover(feature);
      } 
    });
        
     $.each([sliderHeight, sliderCount, sliderLowerDate, sliderHigherDate], function(i, id){
     	var idCssSelector = "#" + id;
     	$(idCssSelector).bind('slidestop input', function(chngEvt){
		    function ltCompareFloats(comparable, parseable) {
			    return comparable < parseFloat(parseable);
		    };

		    $.each(pointVector.getFeatures(),  function(i, e){ 
		        var filter = filterObj[i];
		        function checkFor(comparable, filterName, compareFunc){
			      var result = false;
			      if(chngEvt.target.id == filterName){
				    if(compareFunc(comparable, chngEvt.target.value)){
					    result = true;
					    e.setStyle(invisble);
	                            } 
				    filter[chngEvt.target.id] = result;
			      } 
			      return result;
	                };
		        checkFor(e.B.Hoehe, sliderHeight, ltCompareFloats) || checkFor(e.B.cAll, sliderCount, ltCompareFloats) || checkFor(e.B.von, sliderLowerDate, function(a,b){return a < new Date(parseInt(b), 0, 1);}) || checkFor(e.B.bis, sliderHigherDate, function(a,b){return a > new Date(parseInt(b), 0, 1);})

		        if(!filter.isFilteredOut()) {
			    e.setStyle(styleFunction(e));
		        } 
			
		    });
     	});	
     });
});
