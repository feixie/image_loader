<template>
  <v-app>
    <v-app-bar app>
      <v-toolbar-title class="headline text-uppercase">
        <span class="font-weight-light">Image Loader</span>
      </v-toolbar-title>
    </v-app-bar>
<v-content>
      <ImageLoader/>
    </v-content>
  </v-app>
</template>

<script>
import ImageLoader from './components/ImageLoader';

export default {
  name: 'App',

  components: {
    ImageLoader,
  },

  data: () => ({
    //
  }),
};
</script>
