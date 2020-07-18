 // d3.json('http://127.0.0.1:5000/gspc/').then(function(data) {});

 //  d3.select("#selected-dropdown").text("first");

 d3.select("select")
     .on("change", function(d) {
         var selected = d3.select("#d3-dropdown").node().value;
         console.log(selected);
     });

 var targ_url = 'http://127.0.0.1:5000/n225/';

 anychart.onDocumentReady(function() {
     anychart.data.loadJsonFile(
         targ_url,
         function(data) {

             // create data table on loaded data
             var dataTable = anychart.data.table();
             dataTable.addData(data);

             // map loaded data for the ohlc series
             var mapping = dataTable.mapAs({
                 open: 1,
                 high: 2,
                 low: 3,
                 close: 4
             });

             // map loaded data for the scroller
             var scrollerMapping = dataTable.mapAs();
             scrollerMapping.addField('value', 5);

             // create stock chart
             var chart = anychart.stock();

             // create first plot on the chart
             var plot = chart.plot(0);

             // set grid settings
             plot.yGrid(true).xGrid(true).yMinorGrid(true).xMinorGrid(true);

             // create EMA indicators with period 50
             plot
                 .ema(dataTable.mapAs({
                     value: 4
                 }))
                 .series()
                 .stroke('1.5 #455a64');

             var series = plot.candlestick(mapping);
             series.name('TERRY');
             series.legendItem().iconType('rising-falling');

             // create scroller series with mapped data
             chart.scroller().candlestick(mapping);

             // set chart selected date/time range
             chart.selectRange('2007-01-03', '2007-05-20');

             // set container id for the chart
             chart.container('container');
             // initiate chart drawing
             chart.draw();

             // create range picker
             var rangePicker = anychart.ui.rangePicker();
             // init range picker
             rangePicker.render(chart);

             // create range selector
             var rangeSelector = anychart.ui.rangeSelector();
             // init range selector
             rangeSelector.render(chart);
         }

     );
 });