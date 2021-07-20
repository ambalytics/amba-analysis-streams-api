import {createRouter, createWebHashHistory} from 'vue-router'
import Home from '../views/Home.vue'
import Event from '../views/Event.vue'
import Publications from '../views/Publications.vue'
import Publication from '../views/Publication.vue'

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

const router = createRouter({
    history: createWebHashHistory(),
    routes
})

export default router
