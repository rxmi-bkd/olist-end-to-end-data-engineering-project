use olist;

drop table if exists orders_header_fact;
drop table if exists orders_detail_fact;
drop table if exists sellers_dim;
drop table if exists customers_dim;
drop table if exists dates_dim;
drop table if exists products_dim;

create table sellers_dim
(
    seller_id              varchar(255) null,
    seller_zip_code_prefix varchar(255) null,
    seller_city            varchar(255) null,
    seller_state           varchar(255) null,
    seller_key             int auto_increment primary key
);

create table customers_dim
(
    customer_id              varchar(255) null,
    customer_unique_id       varchar(255) null,
    customer_zip_code_prefix varchar(255) null,
    customer_city            varchar(255) null,
    customer_state           varchar(255) null,
    customer_key             int auto_increment primary key
);

create table dates_dim
(
    day      int        not null,
    month    int        not null,
    year     int        not null,
    date_key varchar(8) not null primary key
);

create table products_dim
(
    product_id                 varchar(255) null,
    product_category_name      varchar(255) null,
    product_name_lenght        int          null,
    product_description_lenght int          null,
    product_photos_qty         int          null,
    product_weight_g           int          null,
    product_length_cm          int          null,
    product_height_cm          int          null,
    product_width_cm           int          null,
    product_key                int auto_increment primary key
);

create table orders_header_fact
(
    nb_items      int    null,
    total_price   double null,
    total_freight double null,
    total_order   double null,
    customer_key  int    null,
    date_order    varchar(8),
    order_id      varchar(255),
    primary key (order_id),
    foreign key (date_order) references dates_dim (date_key),
    foreign key (customer_key) references customers_dim (customer_key)
);

create table orders_detail_fact
(
    price         double     null,
    freight_value double     null,
    order_date    varchar(8) null,
    customer_key  int,
    product_key   int,
    seller_key    int,
    order_item_id int,
    order_id      varchar(255),
    primary key (customer_key, seller_key, product_key, order_id, order_item_id),
    foreign key (customer_key) references customers_dim (customer_key),
    foreign key (product_key) references products_dim (product_key),
    foreign key (seller_key) references sellers_dim (seller_key)
);
