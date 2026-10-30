package server

import "github.com/gofiber/fiber/v3"

func testConfig() fiber.TestConfig {
	return fiber.TestConfig{Timeout: 0, FailOnTimeout: false}
}
