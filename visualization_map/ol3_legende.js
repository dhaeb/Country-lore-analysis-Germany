/* Dependencies: ol3_script.js, jquery  */

/*
creates a label block at legendeElement
label count is the counting of blocks between labels
numberOfSubDivs determines the size of the legende
rangeFrom and rangeTo determine the border values of the legende scale
*/
var createLegende = function(legendeElement, numberOfSubDivs, labelCount, rangeFrom, rangeTo){
    for(i=numberOfSubDivs; i>=1;  i--){
        var value = i*(((rangeTo-rangeFrom)/numberOfSubDivs))+rangeFrom;
        var colorArray = toColorScale(value);
        var color = 'rgb(' + colorArray.join(',') + ')';
        var div = $("<div>").css("backgroundColor", color).addClass("legendeBlock");
        if((i)%labelCount == 0){
            div.addClass("legendeLabel");   
            addLabel(div, value)
        }
        $(legendeElement).append(div);
    }
}

var addLabel = function(element, value){
    $("<div>").addClass("legendeTextBlock").text(value).appendTo(element);
}


