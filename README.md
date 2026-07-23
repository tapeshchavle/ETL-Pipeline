<p align="center">
  <img src="https://img.shields.io/badge/Spring%20Boot-3.4.4-6DB33F?style=for-the-badge&logo=springboot&logoColor=white" alt="Spring Boot"/>
  <img src="https://img.shields.io/badge/Java-17-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java"/>
  <img src="https://img.shields.io/badge/MongoDB-Atlas-47A248?style=for-the-badge&logo=mongodb&logoColor=white" alt="MongoDB"/>
  <img src="https://img.shields.io/badge/AWS-S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white" alt="AWS S3"/>
  <img src="https://img.shields.io/badge/Razorpay-Payment-0C2451?style=for-the-badge&logo=razorpay&logoColor=white" alt="Razorpay"/>
  <img src="https://img.shields.io/badge/Azure-Deployed-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white" alt="Azure"/>
  <img src="https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/>
  <img src="https://img.shields.io/badge/Swagger-API%20Docs-85EA2D?style=for-the-badge&logo=swagger&logoColor=black" alt="Swagger"/>
</p>

# 🍔 Foodingo — Backend API

**Foodingo** is a production-grade, full-stack food ordering platform backend built with **Spring Boot 3.4**. It powers a seamless food ordering experience with secure authentication, real-time cart management, integrated payment processing via Razorpay, and cloud-native image storage on AWS S3 — all deployed to **Azure Container Apps** with CI/CD via GitHub Actions.

> 🌐 **Live API**: [foodingo-api.azurewebsites.net](https://foodingo-api-awasc8h4d7d7cmft.centralindia-01.azurewebsites.net/)  
> 📖 **API Docs**: [Swagger UI](https://foodingo-api-awasc8h4d7d7cmft.centralindia-01.azurewebsites.net/swagger-ui/index.html)

---

## 📋 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [API Documentation](#-api-documentation)
- [Getting Started](#-getting-started)
- [Environment Variables](#-environment-variables)
- [Deployment](#-deployment)
- [Frontend Clients](#-frontend-clients)
- [Contributing](#-contributing)

---

## 🏗 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Layer                             │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│   │  Foodingo    │  │  Admin       │  │  Mobile App          │  │
│   │  Web App     │  │  Dashboard   │  │  (Android)           │  │
│   │  (React)     │  │  (React)     │  │                      │  │
│   └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
└──────────┼─────────────────┼─────────────────────┼──────────────┘
           │                 │                     │
           └─────────────────┼─────────────────────┘
                             │  HTTPS + JWT
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Azure Container Apps                          │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │               Spring Boot 3.4 (Java 17)                   │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐ │ │
│  │  │   Auth   │ │   Food   │ │   Cart   │ │    Order     │ │ │
│  │  │Controller│ │Controller│ │Controller│ │  Controller  │ │ │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────┬───────┘ │ │
│  │       │             │            │              │         │ │
│  │  ┌────┴─────────────┴────────────┴──────────────┴───────┐ │ │
│  │  │              Service Layer (Business Logic)          │ │ │
│  │  │  UserService │ FoodService │ CartService │ OrderSvc  │ │ │
│  │  └────┬─────────────┬────────────┬──────────────┬───────┘ │ │
│  │       │             │            │              │         │ │
│  │  ┌────┴─────────────┴────────────┴──────────────┴───────┐ │ │
│  │  │             Repository Layer (MongoDB)               │ │ │
│  │  └──────────────────────────────────────────────────────┘ │ │
│  │                                                           │ │
│  │  ┌──────────────────┐  ┌──────────────────────────────┐   │ │
│  │  │  Security Layer  │  │       Cross-Cutting           │   │ │
│  │  │  JWT Filter      │  │  CORS • Validation • S3      │   │ │
│  │  │  BCrypt Encoder  │  │  Razorpay • Swagger/OpenAPI  │   │ │
│  │  └──────────────────┘  └──────────────────────────────┘   │ │
│  └────────────────────────────────────────────────────────────┘ │
└────────┬────────────────────┬──────────────────────┬────────────┘
         │                    │                      │
         ▼                    ▼                      ▼
┌──────────────┐    ┌──────────────┐       ┌──────────────────┐
│  MongoDB     │    │   AWS S3     │       │    Razorpay      │
│  Atlas       │    │   Bucket     │       │    Payment       │
│  (Database)  │    │   (Images)   │       │    Gateway       │
└──────────────┘    └──────────────┘       └──────────────────┘
```

---

## ✨ Features

### 🔐 Authentication & Authorization
| Feature | Description |
|---------|-------------|
| **User Registration** | Secure sign-up with BCrypt password hashing |
| **JWT Login** | Stateless authentication with JSON Web Tokens (10-hour expiry) |
| **JWT Filter** | Every request is intercepted and validated via `OncePerRequestFilter` |
| **Role-based Access** | Public endpoints (food catalog, register, login) vs. authenticated endpoints (cart, orders) |

### 🍕 Food Catalog Management (Admin)
| Feature | Description |
|---------|-------------|
| **Add Food Item** | Multipart upload — food JSON + image file in a single request |
| **Image Upload to S3** | Food images stored on AWS S3 with public-read ACL, returns CDN URL |
| **List All Foods** | Retrieve the full food catalog (public, no auth required) |
| **Get Food by ID** | Fetch a single food item's details |
| **Delete Food Item** | Removes both the MongoDB document and the S3 image object |

### 🛒 Shopping Cart
| Feature | Description |
|---------|-------------|
| **Add to Cart** | Adds a food item (by ID); auto-increments quantity if already present |
| **View Cart** | Returns the current user's cart with food-to-quantity map |
| **Decrease Quantity** | Decrements item count by 1; removes the item when count reaches zero |
| **Remove Item** | Instantly removes a specific item regardless of quantity |
| **Clear Cart** | Deletes the entire cart document for the authenticated user |
| **Auto-clear on Payment** | Cart is automatically cleared after successful Razorpay payment verification |

### 💳 Order & Payment Processing
| Feature | Description |
|---------|-------------|
| **Create Order with Payment** | Creates an order in MongoDB and a corresponding Razorpay payment order in a single transaction |
| **Razorpay Integration** | Server-side order creation with `payment_capture=1` (auto-capture) in INR currency |
| **Payment Verification** | Verifies `razorpay_order_id`, `razorpay_payment_id`, and `razorpay_signature` post-payment |
| **User Order History** | Retrieve all orders for the currently authenticated user |
| **Get Order by ID** | Fetch a specific order's full details |
| **Delete Order** | Remove an order record |
| **Admin: View All Orders** | Admin endpoint to list orders across all users |
| **Admin: Update Order Status** | Patch endpoint to transition order status (e.g., `preparing` → `delivered`) |

### ☁️ Cloud & Infrastructure
| Feature | Description |
|---------|-------------|
| **AWS S3 Integration** | Programmatic file upload/delete via AWS SDK v2 for food images |
| **MongoDB Atlas** | Cloud-hosted NoSQL database with connection string via environment variable |
| **Docker Support** | Multi-stage Dockerfile — Maven build stage + Alpine JRE runtime for minimal image size |
| **Azure CI/CD** | GitHub Actions workflow: build → upload artifact → deploy to Azure Web App on every push to `main` |
| **CORS Configuration** | Dual-layer CORS (Spring Security + WebMvcConfigurer) supporting multiple frontend origins |
| **Swagger/OpenAPI** | Interactive API documentation at `/swagger-ui.html` with JWT Bearer auth support |

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|-----------|
| **Framework** | Spring Boot 3.4.4 |
| **Language** | Java 17 |
| **Database** | MongoDB Atlas (via Spring Data MongoDB) |
| **Authentication** | Spring Security + JWT (JJWT 0.11.5) |
| **Payment Gateway** | Razorpay Java SDK 1.4.1 |
| **File Storage** | AWS S3 (AWS SDK v2.31.23) |
| **API Documentation** | SpringDoc OpenAPI (Swagger UI) 2.8.6 |
| **Build Tool** | Maven (with Maven Wrapper) |
| **Containerization** | Docker (Multi-stage: Maven + Eclipse Temurin 17 JRE Alpine) |
| **CI/CD** | GitHub Actions → Azure Web Apps |
| **Code Generation** | Lombok |
| **Validation** | Spring Boot Starter Validation |

---

## 📂 Project Structure

```
foodingo/
├── .github/
│   └── workflows/
│       └── main_foodingo-api.yml        # CI/CD: Build & deploy to Azure
├── src/
│   └── main/
│       ├── java/com/food/
│       │   ├── FoodingoApplication.java # Spring Boot entry point
│       │   │
│       │   ├── config/                  # ⚙️ Configuration
│       │   │   ├── AWSConfig.java       #    AWS S3 client bean
│       │   │   ├── CorsConfig.java      #    WebMvcConfigurer CORS mappings
│       │   │   ├── OpenApiConfig.java   #    Swagger/OpenAPI configuration
│       │   │   └── SecurityConfig.java  #    Spring Security + JWT filter chain
│       │   │
│       │   ├── controller/              # 🌐 REST Controllers
│       │   │   ├── AuthController.java  #    POST /api/user/login
│       │   │   ├── CartController.java  #    CRUD /api/cart
│       │   │   ├── FoodController.java  #    CRUD /api/foods
│       │   │   ├── HomeController.java  #    GET / (health check)
│       │   │   ├── OrderController.java #    CRUD /api/orders
│       │   │   └── UserController.java  #    POST /api/user/register
│       │   │
│       │   ├── entity/                  # 📦 MongoDB Documents
│       │   │   ├── CartEntity.java      #    Cart with userId + items map
│       │   │   ├── FoodEntity.java      #    Food with S3 imageUrl
│       │   │   ├── OrderEntity.java     #    Order with Razorpay fields
│       │   │   └── UserEntity.java      #    User with BCrypt password
│       │   │
│       │   ├── filters/                 # 🔒 Security Filters
│       │   │   └── JwtAuthenticationFilter.java  # Bearer token validation
│       │   │
│       │   ├── io/                      # 📨 DTOs (Request/Response)
│       │   │   ├── AuthenticationRequest.java
│       │   │   ├── AuthenticationResponse.java
│       │   │   ├── CartRequest.java
│       │   │   ├── CartResponse.java
│       │   │   ├── FoodRequest.java
│       │   │   ├── FoodResponse.java
│       │   │   ├── OrderItem.java
│       │   │   ├── OrderRequest.java
│       │   │   ├── OrderResponse.java
│       │   │   ├── UserRequest.java
│       │   │   └── UserResponse.java
│       │   │
│       │   ├── repository/              # 🗄️ Data Access Layer
│       │   │   ├── AuthenticationFacade.java
│       │   │   ├── CartRepository.java
│       │   │   ├── FoodRepository.java
│       │   │   ├── OrderRepository.java
│       │   │   └── UserRepository.java
│       │   │
│       │   ├── service/                 # 💼 Business Logic (Interfaces)
│       │   │   ├── AppUserDetailsService.java  # UserDetailsService impl
│       │   │   ├── AuthenticationFacadeImpl.java
│       │   │   ├── CartService.java
│       │   │   ├── FoodService.java
│       │   │   ├── OrderService.java
│       │   │   ├── UserService.java
│       │   │   └── impl/               # Service Implementations
│       │   │       ├── CartServiceImpl.java
│       │   │       ├── FoodServiceImpl.java
│       │   │       ├── OrderServiceImpl.java
│       │   │       └── UserServiceImpl.java
│       │   │
│       │   └── util/                    # 🔧 Utilities
│       │       └── JwtUtil.java         #    JWT generation & validation
│       │
│       └── resources/
│           └── application.properties   # App config (env-driven)
│
├── Dockerfile                           # Multi-stage Docker build
├── pom.xml                              # Maven dependencies
├── .env.example                         # Environment variable template
├── .gitignore
└── README.md
```

---

## 📖 API Documentation

### Interactive Swagger UI

Once the application is running, access the full interactive API documentation at:

- **Local**: [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html)
- **Production**: [https://foodingo-api-awasc8h4d7d7cmft.centralindia-01.azurewebsites.net/swagger-ui/index.html)

> 💡 **Tip**: Use the **Authorize** button in Swagger UI to paste your JWT token and test authenticated endpoints.

### API Endpoints Summary

#### 🔐 Authentication (`/api/user`)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `POST` | `/api/user/register` | ❌ | Register a new user account |
| `POST` | `/api/user/login` | ❌ | Authenticate and receive JWT token |

#### 🍕 Food Catalog (`/api/foods`)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `POST` | `/api/foods/add` | ✅ | Add a new food item with image (multipart) |
| `GET` | `/api/foods` | ❌ | List all food items |
| `GET` | `/api/foods/{id}` | ❌ | Get a specific food item |
| `DELETE` | `/api/foods/delete/{id}` | ✅ | Delete a food item and its S3 image |

#### 🛒 Cart (`/api/cart`)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `POST` | `/api/cart` | ✅ | Add a food item to cart |
| `GET` | `/api/cart` | ✅ | View current user's cart |
| `POST` | `/api/cart/remove` | ✅ | Decrease item quantity by 1 |
| `DELETE` | `/api/cart/delete-cart` | ✅ | Remove a specific item from cart |
| `DELETE` | `/api/cart/delete` | ✅ | Clear the entire cart |

#### 💳 Orders (`/api/orders`)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `POST` | `/api/orders/create` | ✅ | Create order + Razorpay payment |
| `POST` | `/api/orders/verify` | ✅ | Verify Razorpay payment callback |
| `GET` | `/api/orders` | ✅ | Get authenticated user's orders |
| `GET` | `/api/orders/{orderId}` | ✅ | Get a specific order |
| `DELETE` | `/api/orders/{orderId}` | ✅ | Delete an order |
| `GET` | `/api/orders/all` | ❌ | **Admin**: List all orders |
| `PATCH` | `/api/orders/status/{orderId}` | ✅ | **Admin**: Update order status |

---

## 🚀 Getting Started

### Prerequisites

- **Java 17** or higher
- **Maven 3.8+** (or use the included Maven Wrapper)
- **MongoDB Atlas** account (or local MongoDB instance)
- **AWS Account** with S3 bucket configured
- **Razorpay Account** with API keys

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/foodingo.git
cd foodingo
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials:

```env
# MongoDB Connection
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/foodingo?retryWrites=true&w=majority

# JWT Secret (use a strong, random string)
JWT_SECRET=your-super-secret-jwt-key-min-256-bits

# AWS S3 Configuration
AWS_ACCESS_KEY=AKIA...
AWS_SECRET_KEY=wJalrXUtnFEMI/K7MDENG...
AWS_S3_BUCKET=foodingo-images
AWS_REGION=ap-south-1

# Razorpay Configuration
RAZORPAY_KEY=rzp_live_...
RAZORPAY_SECRET=your-razorpay-secret
```

### 3. Run the Application

```bash
# Using Maven Wrapper
./mvnw spring-boot:run

# Or with Maven
mvn spring-boot:run
```

The API will start on **http://localhost:8080**.

### 4. Access Swagger UI

Navigate to [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html) to explore and test all endpoints.

### 5. Run with Docker

```bash
# Build the Docker image
docker build -t foodingo-api .

# Run the container
docker run -p 8080:8080 \
  -e MONGODB_URI="your-mongodb-uri" \
  -e JWT_SECRET="your-jwt-secret" \
  -e AWS_ACCESS_KEY="your-aws-key" \
  -e AWS_SECRET_KEY="your-aws-secret" \
  -e AWS_S3_BUCKET="your-bucket" \
  -e AWS_REGION="ap-south-1" \
  -e RAZORPAY_KEY="your-razorpay-key" \
  -e RAZORPAY_SECRET="your-razorpay-secret" \
  foodingo-api
```

---

## 🔐 Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MONGODB_URI` | ✅ | MongoDB Atlas connection string |
| `JWT_SECRET` | ✅ | Secret key for JWT signing (HS256) |
| `AWS_ACCESS_KEY` | ✅ | AWS IAM access key for S3 |
| `AWS_SECRET_KEY` | ✅ | AWS IAM secret key for S3 |
| `AWS_S3_BUCKET` | ✅ | S3 bucket name for food images |
| `AWS_REGION` | ❌ | AWS region (default: `ap-south-1`) |
| `RAZORPAY_KEY` | ✅ | Razorpay API key |
| `RAZORPAY_SECRET` | ✅ | Razorpay API secret |

---

## 🚢 Deployment

### Azure Web App (Production)

The project uses **GitHub Actions** for automated CI/CD deployment:

1. **On push to `main`** → Triggers the workflow
2. **Build stage** → Compiles the JAR with Maven (Java 17)
3. **Deploy stage** → Uploads the JAR artifact and deploys to Azure Web App (`foodingo-api`)

**Workflow file**: [`.github/workflows/main_foodingo-api.yml`](.github/workflows/main_foodingo-api.yml)

### Docker Deployment

The multi-stage Dockerfile ensures minimal image size:

```
Stage 1: maven:3.8.5-openjdk-17    → Build FAT JAR
Stage 2: eclipse-temurin:17-jre-alpine → Run with minimal JRE (~180MB)
```

---

## 🖥 Frontend Clients

Foodingo backend supports multiple frontend applications:

| Client | URL | Description |
|--------|-----|-------------|
| **Customer Web App** | [foodingo.tapesh.me](https://foodingo.tapesh.me) | React-based food ordering interface |
| **Admin Dashboard** | [admin-foodingo.tapesh.me](https://admin-foodingo.tapesh.me) | Admin panel for order & food management |
| **Mobile App** | Android (Native) | Mobile ordering experience |

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  Built with ❤️ by <a href="https://tapesh.me">Tapesh Chavle</a>
</p>
