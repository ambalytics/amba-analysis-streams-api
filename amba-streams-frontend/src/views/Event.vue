<template>
    <div class="post">
        <div v-if="loading" class="loading">
            Loading...
        </div>

        <div v-if="error" class="error">
            {{ error }}
        </div>

        <div v-if="post" class="content">
      <pre v-for="item in post" v-bind:key="item._id">
          <code v-html="this.prettyPrint(item)"></code>
        </pre>
        </div>
    </div>
</template>

<script>
    import EventService from "../services/EventService";

    // load n (input) newest tweets
    // auto refresh (option) every n second

    export default {
        name: 'Event',
        data() {
            return {
                loading: false,
                post: null,
                error: null
            }
        },
        created() {
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

                EventService.newest()
                    .then(response => {
                        this.post = response.data;
                        console.log(response.data);
                        this.loading = false;
                    })
                    .catch(e => {
                        console.log(e);
                    });
            },
            prettyPrint(object) {
                var jsonLine = /^( *)("[\w]+": )?("[^"]*"|[\w.+-]*)?([,[{])?$/mg;
                var replacer = function (match, pIndent, pKey, pVal, pEnd) {
                    var key = '<span class="json-key" style="color: brown">',
                        val = '<span class="json-value" style="color: navy">',
                        str = '<span class="json-string" style="color: olive">',
                        r = pIndent || '';
                    if (pKey)
                        r = r + key + pKey.replace(/[": ]/g, '') + '</span>: ';
                    if (pVal)
                        r = r + (pVal[0] == '"' ? str : val) + pVal + '</span>';
                    return r + (pEnd || '');
                };

                if (object === undefined) return "";
                return JSON.stringify(object, null, 3)
                    .replace(/&/g, '&amp;').replace(/\\"/g, '&quot;')
                    .replace(/</g, '&lt;').replace(/>/g, '&gt;')
                    .replace(jsonLine, replacer);
            }
        }
    }
</script>
<style type="text/css">
    body {
        background: #efefef;
    }

    pre {
        background-color: ghostwhite;
        border: 1px solid silver;
        padding: 10px 20px;
        margin: 20px;
        border-radius: 4px;
        width: 1500px;
        margin-left: auto;
        margin-right: auto;
    }

  code {
    text-align: left;
    display: inline-block;
    width: 1200px;
  }

  span.json-string {
    display: inline-block;
    white-space: normal;
  }
</style>