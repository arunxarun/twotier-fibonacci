// main.js


        
$(document).ready(function() {
	
	var t = $('#messages').DataTable( {
        "order": [[ 2, "desc" ]]
    });
	

	function loadTable() {
		$.get('/inprocess',function(data, status) {
			
			console.log(data);
			var obj = $.parseJSON(data);
			t.rows().remove();
			$.each(obj, function() {
				val = 0;
				if (this['fibData']['fibValue'] == -1){
					val = 'in progress';
				}
				else {
					
					val = this['fibData']['fibValue'];
				}
				t.row.add([this['fibData']['fibId'],val,this['formattedStartDate'],this['runTime']]).draw();
			});
						
		});
	}
	
		
	
	loadTable();
	setInterval(function(){ loadTable()},2000);
	
	console.log("ready")
});



