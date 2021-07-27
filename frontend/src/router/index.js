import Vue from 'vue'
import VueRouter from 'vue-router'
import Home from '../views/Home.vue'
import Event from "../views/Event";
import Publications from "../views/Publications";
import Publication from "../views/Publication";

Vue.use(VueRouter)

const routes = [
   {
        path: '/',
        name: 'Home',
        component: Home
    },
    {
        path: '/event',
        name: 'Event',
        component: Event
    },
    {
        path: '/publications',
        name: 'Publications',
        component: Publications
    },
    {
        path: '/publication',
        name: 'Publication',
        component: Publication
    },
    {
        path: '/about',
        name: 'About',
        // route level code-splitting
        // this generates a separate chunk (about.[hash].js) for this route
        // which is lazy-loaded when the route is visited.
        component: () => import(/* webpackChunkName: "about" */ '../views/About.vue')
    }
]

const router = new VueRouter({
  routes
})

export default router
