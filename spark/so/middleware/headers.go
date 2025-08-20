package middleware

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/metadata"
)

func GetClientIpFromHeader(ctx context.Context, xffClientIpPosition int) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("no metadata found")
	}

	// The last IP before the load balancer adds internal IP addresses is the IP
	// of the client connecting to the load balancer. Anything before that is
	// untrustworthy. Unfortunately, different load balancers may add additional
	// IPs after the client, so the exact location of the client IP is
	// configurable for the given SO's infrastructure.
	if xff := md.Get("x-forwarded-for"); len(xff) > 0 {
		ips := strings.Split(xff[0], ",")
		if len(ips) > 0 && xffClientIpPosition >= 0 && xffClientIpPosition < len(ips) {
			return strings.TrimSpace(ips[len(ips)-xffClientIpPosition-1]), nil
		}
	}

	return "", errors.New("no client IP found in header")
}
