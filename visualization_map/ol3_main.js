/*
Dependencies: ol3_script.js, ol3_dialog.js, jquery
*/

var test;

$(function(){
    //createLegende($("#legende"), 100, 25, 0.0, 1.0);
    createLegende($("#legende"), 200, 25, -1.0, 1.0)
    showDialog(function(content){
        run(JSON.parse(content));
    });
});
