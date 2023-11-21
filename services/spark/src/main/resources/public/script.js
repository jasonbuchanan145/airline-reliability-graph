document.getElementById("routeForm").addEventListener("submit", function(event){
    event.preventDefault();
    const submitButton = document.getElementById("submit");
    const spinner = document.getElementById("loadingIndicator");
    submitButton.disabled = true;
    spinner.style='';
    const origin = document.getElementById("origin").value;
    const destination = document.getElementById("destination").value;
    const url = `/route?origin=${encodeURIComponent(origin)}&dest=${encodeURIComponent(destination)}`;

    fetch(url)
        .then(response => response.json())
        .then(data => {
            populateTable('leastDelayedDirect', data.leastDelayedDirect);
            populateTable('leastDelayedOneHop', data.leastDelayedOneHop);
            renderTimingData(data)
        })
        .catch(error => {
            console.error('Error:', error)
            document.getElementById('error').textContent ='An error occurred with this search. Please try a different origin or destination'+error
        }).finally(()=>{
            submitButton.disabled = false;
            spinner.style.display = 'none';
            document.getElementById("timingList").style=''
            document.getElementById("timingCover").style='display:none'
        });
});

document.addEventListener('DOMContentLoaded',initializeOriginDropDown)
document.addEventListener('DOMContentLoaded',initializeDestDropDown)

function initializeOriginDropDown(){
    fetch('/origins')
            .then(response => response.json())
            .then(data => populateDatalist('origin-options', data))
}

function initializeDestDropDown(){
    fetch('/destinations')
            .then(response => response.json())
            .then(data => populateDatalist('dest-options', data))
}

function populateDatalist(datalistId, options) {
    const datalist = document.getElementById(datalistId);
    datalist.innerHTML = '';
    options.forEach(option => {
        const opt = document.createElement('option');
        opt.value = option;
        datalist.appendChild(opt);
    });
}

function renderTimingData(data){
    document.getElementById("timeToCalculateDirectRoutes").textContent=data.timeToCalculateDirectRoutes/1000;
    document.getElementById("timeToCalculateOneStopRoutes").textContent=data.timeToCalculateOneStopRoutes/1000;
    document.getElementById("totalTime").textContent=data.totalTime/1000;

}

function populateTable(tableId, routes) {
    const table = document.getElementById(tableId);
    // clear the table
    table.innerHTML = '';
    if(routes ==null || routes.length === 0) {
        table.innerHTML = '<tr><td>No data available</td></tr>';
        return;
    }
    if(tableId == "leastDelayedOneHop"){
        let headerRow = '<tr><th>Origin</th><th>Layover</th><th>Final destination</th>'+
        '<th>Carrier</th><th>Delay percentage to layover</th><th>Percentage canceled to layover</th>'+
        '<th>Average delay to layover</th><th>Number of flights to the layover</th> '
        + '<th>Percentage canceled to final</th><th>Delay percentage to final</th> <th>Average delay to final</th><th>Number of flights to final</th>'
        '</tr>';
        table.innerHTML = headerRow;
        routes.forEach(route => {
            let row = `<tr>
                    <td>${route[0].origin}</td>
                    <td>${route[0].dest}</td>
                    <td>${route[1].dest}</td>
                    <td>${route[0].carrierName}</td>
                    <td>${route[0].percentageDelayedLongerThan15}</td>
                    <td>${route[0].percentageCancelled}</td>
                    <td>${route[0].avgDelayLongerThan15}</td>
                    <td>${route[0].numFlights}</td>
                    <td>${route[1].percentageCancelled}
                    <td>${route[1].percentageDelayedLongerThan15}</td>
                    <td>${route[1].avgDelayLongerThan15}</td>
                    <td>${route[1].numFlights}</td>
                </tr>`;
                table.innerHTML += row;
        });
    }else{
         let headerRow = '<tr><th>Origin</th><th>Final Destination</th>'+
            '<th>Carrier</th><th>Percentage delayed</th>'+
            '<th>Average delay</th><th>Percentage canceled</th><th>Number of flights</th>'+
             '</tr>';
                table.innerHTML = headerRow;
                routes.forEach(flight => {
                    let row = `<tr>
                            <td>${flight.origin}</td>
                            <td>${flight.dest}</td>
                            <td>${flight.carrierName}</td>
                            <td>${flight.percentageDelayedLongerThan15}</td>
                            <td>${flight.avgDelayLongerThan15}</td>
                             <td>${flight.percentageCancelled}</td>
                            <td>${flight.numFlights}</td>
                        </tr>`;
                        table.innerHTML += row;
                });
    }
}
