/* Dependencies: ol3_script.js, jquery  */

/*
creates a label block at legendeElement
label count is the counting of blocks between labels
numberOfSubDivs determines the size of the legende
rangeFrom and rangeTo determine the border values of the legende scale
*/
var createLegende = function(legendeElement, numberOfSubDivs, labelCount, rangeFrom, rangeTo){
    var sliderTop = createSliderTop();
    $(legendeElement).append(sliderTop);

    var scale = createScale(numberOfSubDivs, labelCount, rangeFrom, rangeTo);
    $(legendeElement).append(scale);
 
    var sliderBottom = createSliderBottom();
    $(legendeElement).append(sliderBottom);
}

var addLabel = function(element, value){
    $("<div>").addClass("legendeTextBlock").text(value).appendTo(element);
}

var createScale = function(numberOfSubDivs, labelCount, rangeFrom, rangeTo){
    var scale = $("<div>").addClass("scaleDiv");
    addScaleBlocks(scale, numberOfSubDivs, labelCount, rangeFrom, rangeTo);
    return scale;
}

var addScaleBlocks = function(scale, numberOfSubDivs, labelCount, rangeFrom, rangeTo){
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

var resetScale = function(legendeElement, numberOfSubDivs, labelCount, rangeFrom, rangeTo){
    var scale = $(legendeElement).children(".scaleDiv");
    scale.children().remove();
    addScaleBlocks(scale, numberOfSubDivs, labelCount, rangeFrom, rangeTo);
}

var createSliderTop = function(){
    var div = $("<div>").addClass("scale-slider-top").addClass("scale-slider");
    var hr = $("<hr>").addClass("scale-slider-line");
    div.append(hr);
    return div;
}   

var createSliderBottom = function(){
    var div = $("<div>").addClass("scale-slider-bottom").addClass("scale-slider");
    var hr = $("<hr>").addClass("scale-slider-line");
    div.append(hr);
    return div;
}
