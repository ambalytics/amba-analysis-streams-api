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

  top() {
    return http.get("/publication/top");
  }
}

export default new PublicationService();
