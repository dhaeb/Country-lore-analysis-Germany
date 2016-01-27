/*
Dependencies: ol3_script.js, ol3_dialog.js, jquery
*/

var test;

$(function(){
    createLegende($("#legende"), 100, 25)
    showDialog(function(content){
        run(JSON.parse(content));
    });
});
