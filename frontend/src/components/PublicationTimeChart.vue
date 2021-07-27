<template>
    <div class="container">
        <line-chart
                v-if="loaded"
                :chartdata="chartdata"
                :options="options"/>
    </div>
</template>

<script>
    import LineChart from './LineChart.vue'
    import EventService from "../services/EventService";

    export default {
        name: 'PublicationLineChart',
        components: {LineChart},
        data: () => ({
            loaded: false,
            chartdata: null,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    xAxes: [{
                        type: 'time'
                    }]
                }
            }
        }),
        mounted() {
            this.loaded = false;
            EventService.forPublication("2419563916")
                .then(response => {
                    console.log(response.data.data);

                    let data = [];
                    let label = [];
                    response.data.data[0].forEach(e => {
                        data.push(e.total);
                        label.push(new Date(e._id.y, e._id.m, e._id.d, e._id.h));
                        // label.push(e._id.h + 'h ' + e._id.d + '.' + e._id.m);
                    });
                    console.log(label)
                    this.chartdata = {
                        labels: label,
                        datasets: [{
                            label: '10.1134/S1560354716060058',
                            backgroundColor: '#f87979',
                            data: data,
                        }],
                    };
                    console.log(this.chartdata)
                    this.loaded = true;
                })
                .catch(e => {
                    console.log(e);
                });
        }
    }
</script>
