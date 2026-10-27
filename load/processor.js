module.exports = {
  pickSku(context, events, done) {
    context.vars.sku = `LOAD-${1 + Math.floor(Math.random() * 20)}`;
    done();
  },
};