<template>
  <div class="home">
    <h1>Publication</h1>

    <div style="display: flex;">
      <label for="field">field</label>
      <input id="field" v-model="field" >

      <label for="limit">limit</label>
      <input id="limit" v-model="limit" type="number">

      <button v-on:click="this.fetchData">get count</button>
    </div>

    <div class="results">
        <div v-for="item in post.data" v-bind:key="item._id">
          {{ this.field }}: {{ item }}
        </div>
    </div>
  </div>
</template>

<script>

// top publications

import PublicationService from "../services/PublicationService";

export default {
  name: 'Publication',
  data () {
    return {
      loading: false,
      post: null,
      error: null,
      field: "obj.data.doi",
      limit: 10
    }
  },
  created () {
      console.log('created');
    // fetch the data when the view is created and the data is
    // already being observed
    this.fetchData()
  },
  watch: {
    // call again the method if the route changes
    '$route': 'fetchData'
  },
  methods: {
    fetchData () {
      console.log('fetch data');

      this.error = this.post = null
      this.loading = true

       PublicationService.getCount(this.field, this.limit)
        .then(response => {
          this.post = response.data;
          console.log(response.data);
          this.loading = false;
        })
        .catch(e => {
          console.log(e);
        });

    }
  }
}
</script>
