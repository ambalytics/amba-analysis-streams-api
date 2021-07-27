<template>
    <v-data-table class="pa-6 elevation-1"
                  :headers="headers"
                  :items="data"
                  :items-per-page="20"
    ></v-data-table>
</template>

<script>

    // top publications

    import PublicationService from "../services/PublicationService";

    export default {
        name: 'Publications',
        data() {
            return {
                headers: [
                    {
                        text: 'Most tweeted Publications',
                        align: 'start',
                        value: 'name',
                    },
                    {text: 'DOI', value: 'doi'},
                    {text: 'Tweets', value: 'count'},
                ],
                data: [],
            }
        }, created() {
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

                PublicationService.top(100)
                    .then(response => {
                        console.log(response.data.data);
                        this.data = response.data.data[0]
                        console.log(this.data);
                    })
                    .catch(e => {
                        console.log(e);
                    });
            },
        }
    }
</script>
