import http from "../http-common";

// https://www.bezkoder.com/vue-3-crud/
class PublicationService {
    getAll() {
        return http.get("/publication");
    }

    get(id) {
        return http.get(`/publication/${id}`);
    }

    getCount(field, limit) {
        return http.get(`/publication/count?field=${field}&limit=${limit}`);
    }

    top(limit) {
        return http.get(`/publication/top?limit=${limit}`);
    }

    types(id = null) {
        return id === null ? http.get("/publication/types") : http.get(`/publication/types?id=${id}`);
    }

    sources(id = null) {
        return id === null ? http.get("/publication/sources") : http.get(`/publication/sources?id=${id}`);
    }

    lang(id = null) {
        return id === null ? http.get("/publication/lang") : http.get(`/publication/lang?id=${id}`);
    }

    // todo boolean original
    authors(id = null) {
        return id === null ? http.get("/publication/authors") : http.get(`/publication/authors?id=${id}`);
    }

    entities(id = null) {
        return id === null ? http.get("/publication/entities") : http.get(`/publication/entities?id=${id}`);
    }

    hashtags(id = null) {
        return id === null ? http.get("/publication/hashtags") : http.get(`/publication/hashtags?id=${id}`);
    }

    timeOfDay(id = null) {
        return id === null ? http.get("/publication/dayhour") : http.get(`/publication/dayhour?id=${id}`);
    }
}

export default new PublicationService();