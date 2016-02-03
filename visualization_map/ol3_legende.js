/* Dependencies: ol3_script.js, jquery  */

var filterWithSliderRange = function(){console.log(currentLegendScaleRange);}

var legendeBlockHeight = 2;
var rangeDivRelation = 100;

var maxLegendScaleRange = {
    from: 0.0,
    to: 0.0
};

var currentLegendScaleRange = {
    from: 0.0,
    to: 0.0
};

var newLegendScaleRange = {
    from: 0.0,
    to: 0.0
};

/*
creates a label block at legendeElement
label count is the counting of blocks between labels
numberOfSubDivs determines the size of the legende
rangeFrom and rangeTo determine the border values of the legende scale
*/
var createLegende = function(legendeElement, labelCount, rangeFrom, rangeTo){
    var sliderTop = createSliderTop();
    $(legendeElement).append(sliderTop);

    var scale = createScale(labelCount, rangeFrom, rangeTo);
    $(legendeElement).append(scale);
 
    var sliderBottom = createSliderBottom();
    $(legendeElement).append(sliderBottom);

    currentLegendScaleRange = JSON.parse(JSON.stringify(maxLegendScaleRange));
}

var addLabel = function(element, value){
    $("<div>").addClass("legendeTextBlock").text(value).appendTo(element);
}

var createScale = function(labelCount, rangeFrom, rangeTo){
    var scale = $("<div>").addClass("scaleDiv");
    addScaleBlocks(scale, labelCount, rangeFrom, rangeTo);
    return scale;
}

var addScaleBlocks = function(scale, labelCount, rangeFrom, rangeTo){
    var numberOfSubDivs = getNumberOfSubDivs(rangeFrom, rangeTo);
    console.log(numberOfSubDivs);
    maxLegendScaleRange.from = rangeFrom;
    maxLegendScaleRange.to = rangeTo;
    for(i=numberOfSubDivs; i>=1;  i--){
        var value = i*(((rangeTo-rangeFrom)/numberOfSubDivs))+rangeFrom;
        var colorArray = toColorScale(value);
        var color = 'rgb(' + colorArray.join(',') + ')';
        var div = $("<div>").css("backgroundColor", color).addClass("legendeBlock");
        if((i)%labelCount == 0){
            div.addClass("legendeLabel");   
            addLabel(div, value)
        }
        $(scale).append(div);
    }
}


var getNumberOfSubDivs = function(rangeFrom, rangeTo){
    var diff = rangeTo - rangeFrom;
    return diff * rangeDivRelation;
}

var resetScale = function(legendeElement, labelCount, rangeFrom, rangeTo){
    var scale = $(legendeElement).children(".scaleDiv");
    scale.children().remove();
    addScaleBlocks(scale, labelCount, rangeFrom, rangeTo);
}

var createSliderTop = function(){
    var slider = createSliderDiv().addClass("scale-slider-top");
    slider.mousedown( getSliderHandler(slider[0], "to") );
    var div = $("<div>").addClass("scale-slider-wrapper").append(slider);
    return div;
}   

var createSliderBottom = function(){
    var slider = createSliderDiv().addClass("scale-slider-bottom");
    slider.mousedown( getSliderHandler(slider[0], "from") );
    var div = $("<div>").addClass("scale-slider-wrapper").append(slider);
    return div;
}

var createSliderDiv = function(){
    var div = $("<div>").addClass("scale-slider");
    var hr = $("<hr>").addClass("scale-slider-line");
    div.append(hr);
    return div;
}

var getSliderHandler = function(element, changeValueName){
    return function(e) {
        e = e || window.event;
        var start = 0, diff = 0;
        if( e.pageY) start = e.pageY;
        else if( e.clientY) start = e.clientY;

        var oldTop = 0;
        if(element.style.top){
            oldTop = parseInt(element.style.top.replace("px", ""));
        }

        element.style.position = 'relative';
        document.body.onmousemove = function(e) {
            e = e || window.event;
            var end = 0;
            if( e.pageY) end = e.pageY;
            else if( e.clientY) end = e.clientY;

            diff = end-start;

            var tempRange = JSON.parse(JSON.stringify(currentLegendScaleRange));
            changeRangeValue(tempRange, changeValueName, diff);

            if(isInMaxRange(tempRange)){
                newLegendScaleRange = tempRange;
                element.style.top = oldTop + diff+"px";
                displayNewSliderRange(diff, changeValueName, element);
            }
        };
        document.body.onmouseup = function() {
            // elem has been moved by diff pixels in the y axis
            setNewSliderRange();
            removeOldSliderRangeDisplay();
            //element.style.position = 'static';
            document.body.onmousemove = document.body.onmouseup = null;
        };
    };
}

var isInMaxRange = function(range){
    return maxLegendScaleRange.from <= range.from && maxLegendScaleRange.to >= range.to && range.to > range.from;
}


var diffToRangeValue = function(diff){
    return diff/legendeBlockHeight/rangeDivRelation;
}

var fractionalDigits = 2;
var powerOfTen = Math.pow(10, fractionalDigits);
var roundToTwoDigits = function(number){
  return Math.round(number * powerOfTen) / powerOfTen;
}

var getNewRangeValue = function(range, valName, diff){
    return roundToTwoDigits(range[valName] - diffToRangeValue(diff));
}

var changeRangeValue = function(range, valueName, diff){
    range[valueName] = getNewRangeValue(range, valueName, diff);
}


var displayNewSliderRange = function(diff, changeValueName, element){
    var newRange = JSON.parse(JSON.stringify(currentLegendScaleRange));
    changeRangeValue(newRange, changeValueName, diff);
    
    removeOldSliderRangeDisplay();
    var div = getRangeDisplayDiv(newRange, changeValueName);
    $("body").append(div);    
    
    var top = element.getBoundingClientRect().top;
    var left = element.getBoundingClientRect().left;

    var width = div.outerWidth();
    div[0].style.left = left-width+"px";
    div[0].style.top = top+"px";
}

var getRangeDisplayDiv = function(range, valueName){
    var text = valueName + ": " + range[valueName]
    return $("<div>").text(text).addClass("RangeDisplay");    
}

var setNewSliderRange = function(){
    currentLegendScaleRange = JSON.parse(JSON.stringify(newLegendScaleRange));
    filterWithSliderRange();
}

var removeOldSliderRangeDisplay = function(){
    $(".RangeDisplay").remove();
}
