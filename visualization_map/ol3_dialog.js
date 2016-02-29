/*
Dependencies: jquery, html5:fileAPI, html5:dialog
*/

String.prototype.replaceAll = function(search, replacement) { // taken from: http://stackoverflow.com/questions/1144783/replacing-all-occurrences-of-a-string-in-javascript
    var target = this;
    return target.replace(new RegExp(search, 'g'), replacement);
};

var readFile = function(file, resultCallback) {
    if (file) {
        var reader = new FileReader();
        reader.onload = (function (file) {
            return function (e) {
                var contents = e.target.result;
                resultCallback(contents);
            };
        })(file);
        reader.readAsText(file);
    } else {
        alert("Failed to load file");
    }
}

var notEmptyInput = function(fileInput){
    return fileInput.files.length != 0;
}

var getInputElement = function(){
    return $("<input type='file' id='fileinput' accept='.json'/>");
}

var getCloseButton = function(dialog, fileInput, resultCallback){
    return $("<button>")
        .text("OK")
        .bind("click", function(){ 
            if(notEmptyInput(fileInput)){
		var file = fileInput.files[0]; //Retrieve the first element from the FileList object
		console.log(file);
		setTitle(file);
                readFile(file, resultCallback); 
                dialog.close(); 
                dialog.parentNode.removeChild(dialog);
            }
        }
    );
}

var setTitle = function(file){
  var extentionRemovedFile = file.name.replaceAll("\\.json|_geo", ""); // remove _geo.json
  document.title = "Analyse cl: " + fromSnailCaseToNormalText(extentionRemovedFile);
};

var fromSnailCaseToNormalText = function(snailCaseText){
  return $.map(snailCaseText.split("_"), function(e){
    return e[0].toUpperCase() + e.slice(1, e.length);
  }).join(' ');
}

var showDialog = function(resultCallback){
    var dialog = $("<dialog><dialog/>");
    var fileInputElement = getInputElement();
    dialog.append(fileInputElement)
        .append(getCloseButton(dialog[0], fileInputElement[0], resultCallback))
        .appendTo(document.body)[0]
        .showModal();
}

