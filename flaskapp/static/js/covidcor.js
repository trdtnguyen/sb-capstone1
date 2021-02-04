function constructTable(json_list, tableid) {

            // Getting the all column names
            var cols = Headers(json_list, tableid);
            var selector = "#" + tableid;
            // Traversing the JSON data
            for (var i = 0; i < json_list.length; i++) {
                var row = $('<tr/>');
                for (var colIndex = 0; colIndex < cols.length; colIndex++)
                {
                    var val = json_list[i][cols[colIndex]];

                    // If there is any key, which is matching
                    // with the column name
                    if (val == null) val = "";
                    /*add commas in to number*/
                    val_str = val.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                    row.append($('<td/>').html(val_str));
                }

                // Adding each row to the table
                $(selector).append(row);
            }
   }
   function Headers(list, tableid) {
      var columns = [];
      var header = $('<tr/>');
      var selector = "#" + tableid;
      for (var i = 0; i < list.length; i++) {
          var row = list[i];
          var count = 0;
          for (var k in row) {
              if ($.inArray(k, columns) == -1) {
                  columns.push(k);

                  // Creating the header
                  var funcName = "sortTable(" + count + ",'" +  tableid + "')"
                  console.log(funcName);
                  header.append($('<th/>').attr({"onclick":funcName}).html(k));
                  count += 1;

              }
          }
      }

      // Appending the header to the table
      $(selector).append(header);
          return columns;
  }

  function sortTable(n, tableid) {
  var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
  console.log('on sortTable, tableid = ' + tableid);
  table = document.getElementById(tableid);
  console.log('on sortTable, table = ' + table);
  //table = $(selector)
  switching = true;
  // Set the sorting direction to ascending:
  dir = "asc";
  /* Make a loop that will continue until
  no switching has been done: */
  while (switching) {
    // Start by saying: no switching is done:
    switching = false;
    rows = table.rows;
    /* Loop through all table rows (except the
    first, which contains table headers): */
    for (i = 1; i < (rows.length - 1); i++) {
      // Start by saying there should be no switching:
      shouldSwitch = false;
      /* Get the two elements you want to compare,
      one from current row and one from the next: */
      x = rows[i].getElementsByTagName("TD")[n];
      y = rows[i + 1].getElementsByTagName("TD")[n];
      /* Check if the two rows should switch place,
      based on the direction, asc or desc: */
      if (dir == "asc") {
        if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
          // If so, mark as a switch and break the loop:
          shouldSwitch = true;
          break;
        }
      } else if (dir == "desc") {
        if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
          // If so, mark as a switch and break the loop:
          shouldSwitch = true;
          break;
        }
      }
    }
    if (shouldSwitch) {
      /* If a switch has been marked, make the switch
      and mark that a switch has been done: */
      rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
      switching = true;
      // Each time a switch is done, increase this count by 1:
      switchcount ++;
    } else {
      /* If no switching has been done AND the direction is "asc",
      set the direction to "desc" and run the while loop again. */
      if (switchcount == 0 && dir == "asc") {
        dir = "desc";
        switching = true;
      }
    }
  }
}