# Caddy S3

This is a super basic dead simple S3 storage module for use with Caddy.

It's based on [this repo](https://github.com/techknowlogick/certmagic-s3), but I made it much more bare-bones and updated it because I was having issues using it with Caddy in govcloud.

Want encrypted certs? Too bad, go to the original repo I linked.
Want auth via AWS Client Access ID/Secret Key ? Too bad, go to the original I linked.

This is as simple as possible so that I can get something working.

I believe the caddyfile syntax to use it should be like this

```plaintext
storage sthree {
  bucket "your-bucket-name"
  region "your-bucket-region"
}
```

## Auth

This module relies on AWS auth that it can get from `context.Background()`. I plan to use this module via an IAM role I have attached to my EC2 instance that is running caddy.

## Testing

I should add absolutey add unit tests, but I haven't.
