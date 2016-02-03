/* Dependencies: constants.js ol3_script.js, jquery  */  

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

function createPopover(feature){
  return  "<table><tr><td>Ort: </td><td>" + feature.get('Ort') + "</td></tr>" 
	+ "<tr><td>Bundesland: </td><td>" + feature.get('Bundesland') + "</td></tr>"
	+ "<tr><td>Anzahl Messjahre: </td><td>" + feature.get('cAll') + "</td></tr>"
	+ "<tr><td>Verhaeltnis: </td><td>" + feature.get('rel') + "</td></tr>"
	+ "<tr><td>Lage: </td><td>" + feature.get('Lage') + "</td></tr>"
	+ "<tr><td>Hoehe: </td><td>" + feature.get('Hoehe') + "</td></tr>"
	+ "<tr><td>Von-Bis: </td><td>" + feature.get('von').getFullYear() + "-" + feature.get('bis').getFullYear() + "</td></tr>"
	+ "</table>";
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

function toColorScale(floating){
  return HSVtoRGB((1 - floating) / 3,1,1);
}

var iconFeature = new ol.Feature({
    geometry: new ol.geom.Point(ol.proj.transform([13, 51], 'EPSG:4326','EPSG:3857')),
    name: "adsf",
    population: 4000,
    rainfall: 500
});

var attribution = new ol.control.Attribution({
  collapsible: true,
  label: 'A',
  collapsed: true,
  tipLabel: 'Filter and copyright info',
  target : 'attribution'
});

var map; 
$(function(){
  map = new ol.Map({
    target: 'map',
    renderer: 'canvas',
    layers: [
      new ol.layer.Tile({
	source: new ol.source.MapQuest({layer : "osm"})
      })
    ],                               
    view: new ol.View({
      center: new ol.proj.transform([10.41,51.30], "EPSG:4326", "EPSG:3857"),
      zoom: 7
    }),
    controls: ol.control.defaults({ attribution: false }).extend([attribution])
  });
});

var run = function(geojsonObject) {
    var pointVector, filterObj = [];

    pointVector = new ol.source.Vector({
        features: (new ol.format.GeoJSON()).readFeatures(geojsonObject,{
	    dataProjection : "EPSG:4326",
	    featureProjection : "EPSG:3857"
	}),
    });     
    
    // init dynamic slider ranges
    var dynamicSliderRanges = {
      minFieldName : "MIN",
      maxFieldName : "MAX",
      getMinOf : function(fieldName){
	return this[fieldName + this.minFieldName];
      },
      getMaxOf : function(fieldName){
	return this[fieldName + this.maxFieldName];
      },
      adjustMinMaxFor : function(fieldName, value){
	var currentMin = this[fieldName + this.minFieldName];
	if(!currentMin | currentMin > value){
	  this[fieldName + this.minFieldName] = value;
	}
	var currentMax = this[fieldName + this.maxFieldName];
	if(!currentMax | currentMax  < value){
	  this[fieldName + this.maxFieldName] = value;
	}
      }
    };
    
    function createNewFilterObj(filters){
      var returnable = {};
      filters.forEach(function(e){
	returnable[e] = false;
      });
      returnable.isFilteredOut = function() {
	  var isFiltered = false;
	  filters.forEach(function(e){
 	    isFiltered = isFiltered || returnable[e];
	  });
	  return isFiltered;
      };
      return returnable;
    }
    
    var isCor = false;
    pointVector.forEachFeature(function(feature){
      // erstelle Filterobjekt für alle Filter, dies es geben soll!
      filterObj.push(createNewFilterObj(filters));
      feature.B.von = new Date(feature.B.von);
      feature.B.bis = new Date(feature.B.bis); 
      
      // cor means neg value
      if(!isCor){
	isCor = feature.B.rel < 0;
      }
      // init ranges
      dynamicSliderRanges.adjustMinMaxFor(sliderHeight, parseFloat(feature.B.Hoehe));
      dynamicSliderRanges.adjustMinMaxFor(sliderCount, feature.B.cAll);
      dynamicSliderRanges.adjustMinMaxFor(sliderLowerDate, feature.B.von.getFullYear());
      dynamicSliderRanges.adjustMinMaxFor(sliderHigherDate, feature.B.bis.getFullYear());
    });      
    
    if(isCor){
      createLegende($("#" + sliderOutcome), 25, -1.0, 1.0, 200);
    } else {
      createLegende($("#" + sliderOutcome), 25, 0, 1.0, 100);
    }
    
    var countDatasets = filterObj.length;
   
    $.each(sliders, function(i,sliderId){
      function adjustSlider(applyable){
	if(!applyable){
	   applyable = function(slider, min, max){
	      slider.value = min;
	   }; 
	}
	var curSlider = $("#" + sliderId)[0];
	var min = dynamicSliderRanges.getMinOf(sliderId);
	var max = dynamicSliderRanges.getMaxOf(sliderId);
	curSlider.min = min;
	curSlider.max = max;
	applyable(curSlider, min, max);
      }
      if(sliderId == sliderHigherDate){
	adjustSlider(function(slider, min, max){
	  slider.value = max;
	});
      } else {
	adjustSlider();
      }
    });
    
    // create stylefunction for the feature points
    
    var defaultRad = Math.abs(2 + 200 / countDatasets); //calculate less size if there are many stations to be shown
    var styleFunction = function (feature, resolution) {
        var maxCountDatasets = dynamicSliderRanges.getMaxOf(sliderCount);
	var rad = feature.get("cAll") * 10 / maxCountDatasets + defaultRad; // the main radius of a station marker can switch from ~defaultRad and 10 + defaultRad
	var percentTrueCl = feature.get('rel');
	var color = toColorScale(percentTrueCl); // choose color from green to red 
	color.push(1); // add the fourth member to the array representing the opacity
	return [createPointStyle(color, rad)];
    };
    
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
        
     $.each(filters, function(i, id){
     	var idCssSelector = "#" + id;
     	$(idCssSelector).bind('slidestop input', function(chngEvt){
		    function ltCompareFloats(comparable, parseable) {
			    return comparable < parseFloat(parseable);
		    };
		    
		    function filteredFeatureOut(feature, compareFunc, comparable, value){
		      if(compareFunc(comparable, value)){
			feature.setStyle(invisble); // filtering out meens set the style to invisble
			return true;
		      } else {
			return false;
		      }
		    };
		    $.each(pointVector.getFeatures(),  function(i, e){ 
		        var filter = filterObj[i];
		        function checkFor(comparable, filterName, compareFunc){
			      var result = false;
			      if(chngEvt.target.id == filterName){
				 if(typeof chngEvt.target.value  == 'undefined'){
				   filter[chngEvt.target.id] = filteredFeatureOut(e, compareFunc, comparable, {
				     "from" : parseFloat($(chngEvt.target).attr("from")),
				     "to"   : parseFloat($(chngEvt.target).attr("to"))
				   });
				 } else {
				   filter[chngEvt.target.id] = filteredFeatureOut(e, compareFunc, comparable, chngEvt.target.value);
				 } 
				 result = true;
			      } 
			      return result;
	                };
		        checkFor(e.B.Hoehe, sliderHeight, ltCompareFloats) || checkFor(e.B.cAll, sliderCount, ltCompareFloats) || checkFor(e.B.von, sliderLowerDate, function(a,b){return a < new Date(parseInt(b), 0, 1);}) || checkFor(e.B.bis, sliderHigherDate, function(a,b){return a > new Date(parseInt(b), 0, 1);}) 
			|| checkFor(e.B.rel, sliderOutcome, function(a,b){
			  return !(e.B.rel > b.from && e.B.rel < b.to);
			});

		        if(!filter.isFilteredOut()) {
			    e.setStyle(styleFunction(e));
		        } 
			
		    });
     	});	
     });
     
     map.addLayer(new ol.layer.Vector({
      	name : "currentData",
      	source: pointVector,
      	style : styleFunction,	
      }));
};

