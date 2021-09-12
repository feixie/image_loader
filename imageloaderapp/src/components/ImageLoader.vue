<template>
  <div class="text-center">
    <v-menu offset-y>
      <template v-slot:activator="{ on, attrs }">
        <v-btn @click="submit()"
          color="primary"
          dark
          v-bind="attrs"
          v-on="on"
        >
          Images
        </v-btn>
      </template>
      <v-list>
        <v-list-item @click="metadata"
          v-for="(item, index) in items"
          :key="index"
        >
          <v-list-item-title>{{ item.name }}</v-list-item-title>
        </v-list-item>
      </v-list>
    </v-menu>
    <h4 v-if="meta.imageNames">Image Name is: {{ meta.imageNames }}</h4>
    <h4 v-if="meta.exposure">Exposure is: {{ meta.exposure }}</h4>
    <h4 v-if="meta.speed">Speed is: {{ meta.speed }}</h4>
    <h4 v-if="meta.hdop">Hdop is: {{ meta.hdop }}</h4>

  </div>
</template>

<script>
  import axios from 'axios'

  export default {
  name: 'ImageLoader',
    data: () => ({
      items: [],
      meta: ''
    }),
    methods: {
    submit () {
      axios.get('http://127.0.0.1:5000/images')
      .then((response) => {
        this.items = response.data
      })
    },
    metadata () {
      // TODO image file name currently hardcoded
      axios.get('http://127.0.0.1:5000/images/ids_band_670_1591033275648.png/metadata')
      .then((response) => {
        this.meta = response.data
      })
    }
  }
}
</script>
