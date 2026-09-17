# `queue_delivery_envelope`

`j` seçicisi JSON, diğer ilk byte değerleri `arbitrary` biçimini seçer. Body
boyutu ve AMQP basic properties doğrudan production RabbitMQ transport
admission sırasına girer: payload bound -> metadata/headroom -> canonical Lily
envelope. Kabul ve her rejection sınıfı bağımsız production gözlemleriyle
tutarlı kalmalıdır.
