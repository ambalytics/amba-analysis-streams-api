<template>
    <v-app>
        <v-app-bar app>
            <v-toolbar-title>
                <router-link to="/" tag="span" style="cursor: pointer">
                    Amba Twitter Analysis
                </router-link>
            </v-toolbar-title>
            <v-spacer></v-spacer>
            <v-toolbar-items class="hidden-xs-only">
                <v-btn text
                       v-for="(rule, index) in $router.options.routes" :key="index" :to="rule.path">
                    <!--          <v-icon left dark>{{ item.icon }}</v-icon>-->
                    {{ rule.name }}
                </v-btn>
            </v-toolbar-items>
        </v-app-bar>
        <v-main>
          <router-view></router-view>
        </v-main>
    </v-app>
</template>

<script>

    export default {
        name: 'amba-event-stream',
    }
</script>

<style lang="scss">

</style>
