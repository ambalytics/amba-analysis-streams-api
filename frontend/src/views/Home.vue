<template>
    <v-container fluid>
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
    import PublicationService from "../services/PublicationService";
    import PublicationTypeChart from "../components/PublicationTypeChart";
    import PublicationLineChart from "../components/PublicationLineChart";

    export default {
        name: 'Home',

        components: {
            PublicationTypeChart, PublicationLineChart,
        },
        methods: {
            sourceGetter() {
                return PublicationService.sources()
            },
            langGetter() {
                return PublicationService.lang()
            },
            timeOfDayGetter() {
                return PublicationService.timeOfDay()
            },
            entitiesGetter() {
                return PublicationService.entities()
            },
            hashtagsGetter() {
                return PublicationService.hashtags()
            }
        }
    }
</script>
