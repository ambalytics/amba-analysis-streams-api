<template>
    <v-container fluid>
        <v-row>
            <v-col cols="4">
                <v-card elevation="2">
                    <v-card-title>10.1134/S1560354716060058</v-card-title>
                    <v-card-text>
                        Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor
                        invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et
                        justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem
                        ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy
                        eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos
                        et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus
                        est Lorem ipsum dolor sit amet.
                    </v-card-text>
                </v-card>
            </v-col>

            <v-col cols="8">
                <v-row>
                    <v-col>
                        <v-card elevation="2">
                            <v-card-title>Publication</v-card-title>
                            <publication-time-chart></publication-time-chart>
                        </v-card>
                    </v-col>
                </v-row>
            </v-col>
        </v-row>

        <v-row>
            <v-col cols="4">
                <v-card elevation="2">
                            <v-card-title>Types</v-card-title>
                    <publication-type-chart :id="id"></publication-type-chart>
                </v-card>
            </v-col>
            <v-col cols="4">
                <v-card elevation="2">
                            <v-card-title>Sources</v-card-title>
                    <publication-type-chart :id="id" :getter="sourceGetter"></publication-type-chart>
                </v-card>
            </v-col>
            <v-col cols="4">
                <v-card elevation="2">
                            <v-card-title>Languages</v-card-title>
                    <publication-type-chart :id="id" :getter="langGetter"></publication-type-chart>
                </v-card>
            </v-col>
        </v-row>
        <v-row>
            <v-col cols="4">
                <v-card elevation="2">
                            <v-card-title>Time of Day</v-card-title>
                    <publication-line-chart :id="id" :getter="timeOfDayGetter"></publication-line-chart>
                </v-card>
            </v-col>
            <v-col cols="4">
                <v-card elevation="2">
                            <v-card-title>Hashtags</v-card-title>
                    <publication-type-chart :id="id" :getter="hashtagsGetter"></publication-type-chart>
                </v-card>
            </v-col>
            <v-col cols="4">
                <v-card elevation="2">
                            <v-card-title>Entities</v-card-title>
                    <publication-type-chart :id="id" :getter="entitiesGetter"></publication-type-chart>
                </v-card>
            </v-col>
        </v-row>
    </v-container>
</template>

<script>

    // top publications
    import PublicationTimeChart from "../components/PublicationTimeChart";
    import PublicationLineChart from "../components/PublicationLineChart";
    import PublicationTypeChart from "../components/PublicationTypeChart";
    import PublicationService from "../services/PublicationService";

    export default {
        name: 'Publication',
        components: {PublicationTimeChart, PublicationTypeChart, PublicationLineChart},
        data: () => ({
            id: "2419563916",
        }),
        methods: {
            sourceGetter(id) {
                return PublicationService.sources(id)
            },
            langGetter(id) {
                return PublicationService.lang(id)
            },
            timeOfDayGetter(id) {
                return PublicationService.timeOfDay(id)
            },
            entitiesGetter(id) {
                return PublicationService.entities(id)
            },
            hashtagsGetter(id) {
                return PublicationService.hashtags(id)
            }
        }
    }
</script>
