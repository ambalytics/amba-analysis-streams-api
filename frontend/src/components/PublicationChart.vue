<script>
    import { Bar } from 'vue-chartjs'
    import EventService from "../services/EventService";

    export default {
        extends: Bar,
       data: () => ({
            chartdata: {
                labels: ['January', 'February'],
                datasets: [
                    {
                        label: 'Data One',
                        backgroundColor: '#f87979',
                        data: [40, 20]
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false
            }
        }),
        mounted() {
            console.log('created');
            // fetch the data when the view is created and the data is
            // already being observed
            this.fetchData()
        },
        methods: {
            fetchData() {
                console.log('fetch data');
                this.error = this.post = null
                this.loading = true

                EventService.forPublication("2419563916")
                    .then(response => {
                        console.log(response.data.data);

                        let data = [];
                        let label = [];
                        response.data.data[0].forEach(e => {
                            data.push(e.total);
                            label.push(e._id.h + 'h ' + e._id.d + '.' + e._id.m);
                        });
                        this.chartdata = {
                            labels: label,
                            datasets: [{
                                label: '10.1134/S1560354716060058',
                                backgroundColor: '#f87979',
                                data: data,
                            }],
                        };
                        console.log(this.chartdata);

                        this.renderChart(this.chartdata, this.options)
                    })
                    .catch(e => {
                        console.log(e);
                    });
            },
        }
    }
</script>s