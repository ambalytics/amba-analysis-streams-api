<template>
    <div class="container">
        <line-chart
                v-if="loaded"
                :chartdata="chartdata"/>
    </div>
</template>

<script>
    import LineChart from './LineChart.vue'
    import PublicationService from "../services/PublicationService";

    export default {
        name: 'PublicationLineChart',
        components: {LineChart},
         props: {
            id: {
                type: String,
                default: null
            },
            getter: {
                type: Function,
                default: PublicationService.types
            },
        },
        data: () => ({
            loaded: false,
            chartdata: null,
        }),
        mounted() {
            this.loaded = false;
            this.getter(this.id)
                .then(response => {
                    console.log(response.data.data);


                    let data = [];
                    let label = [];
                    response.data.data[0].forEach(e => {
                        data.push(e.total);
                        label.push(e._id);
                        // label.push(e._id.h + 'h ' + e._id.d + '.' + e._id.m);
                    });
                    console.log("LINE");
                    console.log(data);
                    console.log(label);
                    this.chartdata = {
                        labels: label,
                        datasets: [{
                            label: '10.1134/S1560354716060058',
                            backgroundColor: [
                                'rgb(248, 121, 121)',
                                'rgb(54, 162, 235)',
                                'rgb(255, 205, 86)',
                                'rgb(92,255,150)'
                            ],
                            hoverOffset: 4,
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
